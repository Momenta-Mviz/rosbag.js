// Copyright (c) 2018-present, Cruise LLC

// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

// @flow

import BagReader, { type Decompress } from "./BagReader";
import { MessageReader } from "./MessageReader";
import ReadResult from "./ReadResult";
import { BagHeader, ChunkInfo, Connection, MessageData } from "./record";
import type { Time } from "./types";
import * as TimeUtil from "./TimeUtil";
import { parseMessageDefinition } from "./parseMessageDefinition";

export type ReadOptions = {|
  decompress?: Decompress,
  noParse?: boolean,
  topics?: string[],
  startTime?: Time,
  endTime?: Time,
  freeze?: ?boolean,
  alignment?: string,
  timeDiffInfo?: any,
  alignWindow?: number,
  topicNameProxy?: {
    prefix: string,
    suffix: string,
  },
|};

// the high level rosbag interface
// create a new bag by calling:
// `const bag = await Bag.open('./path-to-file.bag')` in node or
// `const bag = await Bag.open(files[0])` in the browser
//
// after that you can consume messages by calling
// `await bag.readMessages({ topics: ['/foo'] },
//    (result) => console.log(result.topic, result.message))`
export default class Bag {
  reader: BagReader;
  header: BagHeader;
  connections: { [conn: number]: Connection };
  chunkInfos: ChunkInfo[];
  startTime: ?Time;
  endTime: ?Time;

  // you can optionally create a bag manually passing in a bagReader instance
  constructor(bagReader: BagReader) {
    this.reader = bagReader;
  }

  // eslint-disable-next-line no-unused-vars
  static open = (file: File | string) => {
    throw new Error(
      "This method should have been overridden based on the environment. Make sure you are correctly importing the node or web version of Bag."
    );
  };

  // if the bag is manually created with the constructor, you must call `await open()` on the bag
  // generally this is called for you if you're using `const bag = await Bag.open()`
  async open() {
    this.header = await this.reader.readHeaderAsync();
    const { connectionCount, chunkCount, indexPosition } = this.header;

    const result = await this.reader.readConnectionsAndChunkInfoAsync(indexPosition, connectionCount, chunkCount);

    this.connections = {};

    result.connections.forEach((connection) => {
      this.connections[connection.conn] = connection;
    });

    this.chunkInfos = result.chunkInfos;

    if (chunkCount > 0) {
      this.startTime = this.chunkInfos[0].startTime;
      this.endTime = this.chunkInfos[chunkCount - 1].endTime;
    }
  }

  async readMessages(opts: ReadOptions, callback: (msg: ReadResult<any>) => void) {
    const connections = this.connections;

    let startTime = opts.startTime || { sec: 0, nsec: 0 };
    let endTime = opts.endTime || { sec: Number.MAX_VALUE, nsec: Number.MAX_VALUE };

    const st = { ...startTime };
    const et = { ...endTime };

    const alignment = opts.noParse ? undefined : opts.alignment; // 必须要能解析消息时，才能做对齐
    const alignWindow = opts.alignWindow ? opts.alignWindow : 200; // default 200ms
    // 当有alignment的时候，读数据的时候后找一段（默认从起始时间向后找0.2s）,期望是能把当前帧egopose对应的fusion和vision包括起来
    // start time不变
    if (alignment) {
      if (opts.startTime && startTime.sec >= 1) startTime = TimeUtil.add(startTime, { sec: 0, nsec: -200000000 }); // 稍微往前读一点，防止对齐align的时候找不到对应的egopose

      endTime = TimeUtil.add(endTime, {
        sec: Math.floor(alignWindow / 1000),
        nsec: (alignWindow % 1000) * 1000000,
      });
    }

    let topics =
      opts.topics ||
      Object.keys(connections).map((id: any) => {
        return connections[id].topic;
      });

    const proxyTopicMap = {};
    if (opts.topicNameProxy) {
      // 都为空时直接跳过（好像会有异常，目前还是在前端约束）
      if (opts.topicNameProxy.prefix.length === 0 && opts.topicNameProxy.suffix.length === 0) return;

      const { prefix = "", suffix = "" } = opts.topicNameProxy;

      const proxyedTopicList = [];
      // 获取bag中的全量topic
      const allTopics = Object.keys(connections).map((id: any) => {
        return connections[id].topic;
      });
      // 同时添加prefix和suffix的代理
      topics.forEach((t) => {
        // 对于已订阅的topic，检查
        const proxyName = prefix + t + suffix;
        if (allTopics.includes(proxyName)) {
          // 判断是否全量topic里有代理后的，如有，则记录
          proxyTopicMap[proxyName] = t;
          proxyedTopicList.push(t);
        }
      });

      // 先把被代理的topic移除，不读取
      topics = topics.filter((t) => !proxyedTopicList.includes(t));

      // 然后添加代理topic的读取
      const proxyTopicList = Object.keys(proxyTopicMap);
      proxyTopicList.forEach((i) => topics.push(i));
    }

    const filteredConnections = Object.keys(connections)
      .filter((id: any) => {
        const proxyTopicList = Object.keys(proxyTopicMap);
        return (
          topics.indexOf(connections[id].topic) !== -1 &&
          (proxyTopicList.length > 0 ? proxyTopicList.indexOf(connections[id].topic) !== -1 : true) // 当有proxy时，还需要筛选带proxy的connection
        );
      })
      .map((id) => +id);

    const { decompress = {} } = opts;

    // filter chunks to those which fall within the time range we're attempting to read
    const chunkInfos = this.chunkInfos.filter((info) => {
      return TimeUtil.compare(info.startTime, endTime) <= 0 && TimeUtil.compare(startTime, info.endTime) <= 0;
    });

    function parseMsg(msg: MessageData, chunkOffset: number): ReadResult<any> {
      const connection = connections[msg.conn];
      const { topic } = connection;
      const { data, time: timestamp } = msg;
      let message = null;
      if (!opts.noParse) {
        // lazily create a reader for this connection if it doesn't exist
        connection.reader =
          connection.reader ||
          new MessageReader(parseMessageDefinition(connection.messageDefinition), { freeze: opts.freeze });
        message = connection.reader.readMessage(data);
      }
      return new ReadResult(topic, message, timestamp, data, chunkOffset, chunkInfos.length, opts.freeze);
    }

    const alignmentList = (alignment || "").split(",").filter((i) => i.length > 0);
    const alignmentCache: any[] = [];
    const egoposeAlignment: any[] = [];

    const convertUsTime = (usTime) => {
      const sec = Math.floor(usTime / 1000000);
      const nsec = Math.floor((usTime % 1000000) * 1000);

      return { sec, nsec };
    };

    const convertNsTime = (usTime) => {
      const sec = Math.floor(usTime / 1000000000);
      const nsec = Math.floor(usTime % 1000000000);

      return { sec, nsec };
    };

    const local2UtcDiff = convertNsTime(opts.timeDiffInfo ? opts.timeDiffInfo.diff : 0);

    for (let i = 0; i < chunkInfos.length; i++) {
      const info = chunkInfos[i];
      const messages = await this.reader.readChunkMessagesAsync(
        info,
        filteredConnections,
        startTime,
        endTime,
        decompress
      );
      messages.forEach((msg) => {
        const readResult = parseMsg(msg, i);

        // proxy, 在这里读到代理消息，覆盖被代理消息
        const proxyTopicList = Object.keys(proxyTopicMap);
        if (proxyTopicList.length > 0) {
          const { topic } = readResult;
          if (proxyTopicMap[topic]) readResult.topic = proxyTopicMap[topic]; // 使用proxy对应的topicname而不是实际读到的
        }

        if (!alignment) {
          // 非对齐模式下，正常返回
          callback(readResult);
        } else {
          // 对readResult的落盘时间，按meta时间重新赋值
          const message = readResult.message;
          if (message.meta && message.meta.sensor_timestamp_us) {
            const newStamp = TimeUtil.add(convertUsTime(message.meta.sensor_timestamp_us), local2UtcDiff); // local + diff
            readResult.timestamp = newStamp;
          } else if (message.meta && message.meta.timestamp_us) {
            const newStamp = TimeUtil.add(convertUsTime(message.meta.timestamp_us), local2UtcDiff); // local + diff
            readResult.timestamp = newStamp;
          }

          if (readResult.topic === "/mla/egopose") {
            // egopose单独处理，全部添加
            egoposeAlignment.push(readResult);
          }

          if (
            readResult.topic !== "/mla/egopose" &&
            TimeUtil.compare(readResult.timestamp, st) >= 0 &&
            TimeUtil.compare(readResult.timestamp, et) <= 0
          ) {
            // 仅缓存更新后时间戳落在目标区域内的readResult
            const topic = readResult.topic;
            if ([...alignmentList].includes(topic)) {
              egoposeAlignment.push(readResult);
            } else {
              alignmentCache.push(readResult);
            }
          }
        }
      });
    }

    if (alignment) {
      // alignment模式下，对多读进来的数据做处理，吐出去按meta进行排列之后的数据，目标是返回【meta时间落在目标时间内的消息】
      // 不修改消息中本身的内容（header时间）
      // 修改readResult中的所有timestamp(落盘时间)
      // egopose只保留和alignment topic中最后一条消息最近的那一条

      // 找到最后一条alignment
      const maxAlignment = egoposeAlignment.reduce(
        (max, item) =>
          alignmentList.includes(item.topic) &&
          TimeUtil.isGreaterThan(item.timestamp, max ? max.timestamp : { sec: 0, nsec: 0 })
            ? item
            : max,
        null
      );

      if (!maxAlignment) return;

      // 向前找到比alignment时间小的最近的egopose
      const maxEgopose = egoposeAlignment.reduce((max, item) => {
        return item.topic === "/mla/egopose" &&
          TimeUtil.isGreaterThan(maxAlignment.timestamp, item.timestamp) &&
          TimeUtil.isGreaterThan(item.timestamp, max ? max.timestamp : { sec: 0, nsec: 0 })
          ? item
          : max;
      }, null);

      if (maxEgopose) {
        // 发布egopose，实际表现为抽帧
        callback(maxEgopose);
      }

      // 发布所有alignment
      egoposeAlignment.filter((i) => alignmentList.includes(i.topic)).forEach((j) => callback(j));

      // 发布其他消息
      [...alignmentCache]
        .sort((a, b) => (TimeUtil.isGreaterThan(a.timestamp, b.timestamp) ? 1 : -1)) // 发布前先升序
        .forEach((i) => callback(i));
    }
  }
}
