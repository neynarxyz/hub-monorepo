import { Command } from "@commander-js/extra-typings";
import {
  bytesToHexString,
  getStorageUnitExpiry,
  getStorageUnitType,
  type HubEvent,
  HubInfoRequest,
  type IdRegisterEventBody,
  isCastAddMessage,
  isCastRemoveMessage,
  isIdRegisterOnChainEvent,
  isMergeOnChainHubEvent,
  isSignerMigratedOnChainEvent,
  isSignerOnChainEvent,
  isStorageRentOnChainEvent,
  type Message,
  type SignerEventBody,
  type SignerMigratedEventBody,
  type StorageRentEventBody,
} from "@farcaster/hub-nodejs";
import type { Queue } from "bullmq";
import { readFileSync } from "fs";
import { ok } from "neverthrow";
import * as process from "node:process";
import url from "node:url";
import {
  type DB,
  EventStreamConnection,
  EventStreamHubSubscriber,
  getDbClient,
  getHubClient,
  HubEventProcessor,
  HubEventStreamConsumer,
  type HubSubscriber,
  type MessageHandler,
  MessageReconciliation,
  type MessageState,
  RedisClient,
  type StoreMessageOperation,
} from "../index.ts"; // If you want to use this as a standalone app, replace this import with "@farcaster/shuttle"
import { bytesToHex, farcasterTimeToDate } from "../utils.ts";
import { type AppDb, migrateToLatest, Tables } from "./db.ts";
import {
  BACKFILL_FIDS,
  CONCURRENCY,
  HUB_HOST,
  HUB_SSL,
  MAX_FID,
  POSTGRES_SCHEMA,
  POSTGRES_URL,
  REDIS_URL,
  SHARD_INDEX,
  SUBSCRIBE_RPC_TIMEOUT,
  TOTAL_SHARDS,
  USE_STREAMING_RPCS_FOR_BACKFILL,
} from "./env";
import { log } from "./log.ts";
import { getQueue, getWorker } from "./worker.ts";

const hubId = "shuttle";

export class App implements MessageHandler {
  private readonly db: DB;
  private readonly dbSchema: string;
  private hubSubscriber: HubSubscriber;
  private streamConsumer: HubEventStreamConsumer;
  public redis: RedisClient;
  private readonly hubId;

  constructor(
    db: DB,
    dbSchema: string,
    redis: RedisClient,
    hubSubscriber: HubSubscriber,
    streamConsumer: HubEventStreamConsumer,
  ) {
    this.db = db;
    this.dbSchema = dbSchema;
    this.redis = redis;
    this.hubSubscriber = hubSubscriber;
    this.hubId = hubId;
    this.streamConsumer = streamConsumer;
  }

  static create(
    dbUrl: string,
    dbSchema: string,
    redisUrl: string,
    hubUrl: string,
    totalShards: number,
    shardIndex: number,
    hubSSL = false,
  ) {
    const db = getDbClient(dbUrl, dbSchema);
    const hub = getHubClient(hubUrl, { ssl: hubSSL });
    const redis = RedisClient.create(redisUrl);
    const eventStreamForWrite = new EventStreamConnection(redis.client);
    const eventStreamForRead = new EventStreamConnection(redis.client);
    const shardKey = totalShards === 0 ? "all" : `${shardIndex}`;
    const hubSubscriber = new EventStreamHubSubscriber(
      hubId,
      hub,
      eventStreamForWrite,
      redis,
      shardKey,
      log,
      undefined,
      totalShards,
      shardIndex,
      SUBSCRIBE_RPC_TIMEOUT,
    );
    const streamConsumer = new HubEventStreamConsumer(hub, eventStreamForRead, shardKey);

    return new App(db, dbSchema, redis, hubSubscriber, streamConsumer);
  }

  async onHubEvent(event: HubEvent, txn: DB): Promise<boolean> {
    if (isMergeOnChainHubEvent(event)) {
      const onChainEvent = event.mergeOnChainEventBody.onChainEvent;
      let body = {};
      if (isIdRegisterOnChainEvent(onChainEvent)) {
        body = {
          eventType: onChainEvent.idRegisterEventBody.eventType,
          from: bytesToHex(onChainEvent.idRegisterEventBody.from),
          to: bytesToHex(onChainEvent.idRegisterEventBody.to),
          recoveryAddress: bytesToHex(onChainEvent.idRegisterEventBody.recoveryAddress),
        };
      } else if (isSignerOnChainEvent(onChainEvent)) {
        body = {
          eventType: onChainEvent.signerEventBody.eventType,
          key: bytesToHex(onChainEvent.signerEventBody.key),
          keyType: onChainEvent.signerEventBody.keyType,
          metadata: bytesToHex(onChainEvent.signerEventBody.metadata),
          metadataType: onChainEvent.signerEventBody.metadataType,
        };
      } else if (isStorageRentOnChainEvent(onChainEvent)) {
        body = {
          eventType: getStorageUnitType(onChainEvent),
          expiry: getStorageUnitExpiry(onChainEvent),
          units: onChainEvent.storageRentEventBody.units,
          payer: bytesToHex(onChainEvent.storageRentEventBody.payer),
        };
      } else if (isSignerMigratedOnChainEvent(onChainEvent)) {
        body = {
          migratedAt: onChainEvent.signerMigratedEventBody.migratedAt,
        };
      }

      if (Object.keys(body).length > 0) {
        try {
          await (txn as AppDb)
            .insertInto("onchain_events")
            .values({
              chainId: BigInt(onChainEvent.chainId),
              blockTimestamp: new Date(onChainEvent.blockTimestamp * 1000),
              blockNumber: BigInt(onChainEvent.blockNumber),
              logIndex: onChainEvent.logIndex,
              txHash: onChainEvent.transactionHash,
              type: onChainEvent.type,
              fid: onChainEvent.fid,
              body: body as IdRegisterEventBody | SignerEventBody | StorageRentEventBody | SignerMigratedEventBody,
            })
            .execute();
          log.info(`Recorded OnChainEvent ${onChainEvent.type} for fid  ${onChainEvent.fid}`);
        } catch (e) {
          log.error("Failed to insert onchain event", e);
        }
      }
    }
    return false;
  }

  async handleMessageMerge(
    message: Message,
    txn: DB,
    operation: StoreMessageOperation,
    state: MessageState,
    isNew: boolean,
    wasMissed: boolean,
  ): Promise<void> {
    if (!isNew) {
      // Message was already in the db, no-op
      return;
    }

    const appDB = txn as unknown as AppDb; // Need this to make typescript happy, not clean way to "inherit" table types

    // Example of how to materialize casts into a separate table. Insert casts into a separate table, and mark them as deleted when removed
    // Note that since we're relying on "state", this can sometimes be invoked twice. e.g. when a CastRemove is merged, this call will be invoked 2 twice:
    // castAdd, operation=delete, state=deleted (the cast that the remove is removing)
    // castRemove, operation=merge, state=deleted (the actual remove message)
    const isCastMessage = isCastAddMessage(message) || isCastRemoveMessage(message);
    if (isCastMessage && state === "created") {
      await appDB
        .insertInto("casts")
        .values({
          fid: message.data.fid,
          hash: message.hash,
          text: message.data.castAddBody?.text || "",
          timestamp: farcasterTimeToDate(message.data.timestamp) || new Date(),
        })
        .execute();
    } else if (isCastMessage && state === "deleted") {
      await appDB
        .updateTable("casts")
        .set({
          deletedAt: farcasterTimeToDate(message.data.timestamp) || new Date(),
        })
        .where("hash", "=", message.hash)
        .execute();
    }

    const messageDesc = wasMissed ? `missed message (${operation})` : `message (${operation})`;
    log.info(`${state} ${messageDesc} ${bytesToHexString(message.hash)._unsafeUnwrap()} (type ${message.data?.type})`);
  }

  async start() {
    await this.ensureMigrations();
    // Hub subscriber listens to events from the hub and writes them to a redis stream. This allows for scaling by
    // splitting events to multiple streams
    await this.hubSubscriber.start();

    // Sleep 10 seconds to give the subscriber a chance to create the stream for the first time.
    await new Promise((resolve) => setTimeout(resolve, 10_000));

    log.info("Starting stream consumer");
    // Stream consumer reads from the redis stream and inserts them into postgres
    await this.streamConsumer.start(async (event) => {
      await this.processHubEvent(event);
      return ok({ skipped: false });
    });
  }

  async reconcileFids(fids: number[]) {
    const reconciler = new MessageReconciliation(
      // biome-ignore lint/style/noNonNullAssertion: client is always initialized
      this.hubSubscriber.hubClient!,
      this.db,
      log,
      undefined,
      USE_STREAMING_RPCS_FOR_BACKFILL,
    );
    for (const fid of fids) {
      await reconciler.reconcileMessagesForFid(
        fid,
        async (message, missingInDb, prunedInDb, revokedInDb) => {
          if (missingInDb) {
            await HubEventProcessor.handleMissingMessage(this.db, message, this);
          } else if (prunedInDb || revokedInDb) {
            const messageDesc = prunedInDb ? "pruned" : revokedInDb ? "revoked" : "existing";
            log.info(`Reconciled ${messageDesc} message ${bytesToHexString(message.hash)._unsafeUnwrap()}`);
          }
        },
        async (message, missingInHub) => {
          if (missingInHub) {
            log.info(`Message ${bytesToHexString(message.hash)._unsafeUnwrap()} is missing in the hub`);
          }
        },
      );
    }
  }

  async backfillFids(fids: number[], backfillQueue: Queue) {
    const startedAt = Date.now();
    if (!this.hubSubscriber.hubClient) {
      log.error("Hub client is not initialized");
      throw new Error("Hub client is not initialized");
    }
    if (fids.length === 0) {
      let maxFid = MAX_FID ? parseInt(MAX_FID) : undefined;
      if (!maxFid) {
        const getInfoResult = await this.hubSubscriber.hubClient?.getInfo(HubInfoRequest.create({}));
        if (getInfoResult?.isErr()) {
          log.error("Failed to get max fid", getInfoResult.error);
          throw getInfoResult.error;
        } else {
          maxFid = getInfoResult?._unsafeUnwrap()?.dbStats?.numFidEvents;
          if (!maxFid) {
            log.error("Failed to get max fid");
            throw new Error("Failed to get max fid");
          }
        }
      }
      log.info(`Queuing up fids upto: ${maxFid}`);
      // create an array of arrays in batches of 100 upto maxFid
      const batchSize = 10;
      const fids = Array.from({ length: Math.ceil(maxFid / batchSize) }, (_, i) => i * batchSize).map((fid) => fid + 1);
      for (const start of fids) {
        const subset = Array.from({ length: batchSize }, (_, i) => start + i);
        await backfillQueue.add("reconcile", { fids: subset });
      }
    } else {
      await backfillQueue.add("reconcile", { fids });
    }
    await backfillQueue.add("completionMarker", { startedAt });
    log.info("Backfill jobs queued");
  }

  private async processHubEvent(hubEvent: HubEvent) {
    await HubEventProcessor.processHubEvent(this.db, hubEvent, this);
  }

  async ensureMigrations() {
    const result = await migrateToLatest(this.db, this.dbSchema, log);
    if (result.isErr()) {
      log.error("Failed to migrate database", result.error);
      throw result.error;
    }
  }

  async stop() {
    this.hubSubscriber.stop();
    const lastEventId = await this.redis.getLastProcessedEvent(this.hubId);
    log.info(`Stopped at eventId: ${lastEventId}`);
  }
}

//If the module is being run directly, start the shuttle
if (import.meta.url.endsWith(url.pathToFileURL(process.argv[1] || "").toString())) {
  async function start() {
    log.info(`Creating app connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`);
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    log.info("Starting shuttle");
    await app.start();
  }

  async function backfill() {
    log.info(`Creating app connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`);
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    const fids = BACKFILL_FIDS ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid)) : [];
    log.info(`Backfilling fids: ${fids}`);
    const backfillQueue = getQueue(app.redis.client);
    await app.backfillFids(fids, backfillQueue);

    // Start the worker after initiating a backfill
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);
    await worker.run();
    return;
  }

  async function worker() {
    log.info(`Starting worker connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`);
    const app = App.create(POSTGRES_URL, POSTGRES_SCHEMA, REDIS_URL, HUB_HOST, TOTAL_SHARDS, SHARD_INDEX, HUB_SSL);
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);
    await worker.run();
  }

  // for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  //   process.on(signal, async () => {
  //     log.info(`Received ${signal}. Shutting down...`);
  //     (async () => {
  //       await sleep(10_000);
  //       log.info(`Shutdown took longer than 10s to complete. Forcibly terminating.`);
  //       process.exit(1);
  //     })();
  //     await app?.stop();
  //     process.exit(1);
  //   });
  // }

  const program = new Command()
    .name("shuttle")
    .description("Synchronizes a Farcaster Hub with a Postgres database")
    .version(JSON.parse(readFileSync("./package.json").toString()).version);

  program.command("start").description("Starts the shuttle").action(start);
  program.command("backfill").description("Queue up backfill for the worker").action(backfill);
  program.command("worker").description("Starts the backfill worker").action(worker);

  program.parse(process.argv);
}
