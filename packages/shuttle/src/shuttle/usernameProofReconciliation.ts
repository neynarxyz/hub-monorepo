import {
  type HubRpcClient,
  type UserNameProof,
  UserNameType,
  bytesToHexString,
  fromFarcasterTime,
} from "@farcaster/hub-nodejs";
import { type DB } from "./db";
import { Logger } from "pino";
import { ok, err, Result } from "neverthrow";

export class UsernameProofReconciliation {
  private readonly hubClient: HubRpcClient;
  private readonly db: DB;
  private readonly log: Logger;

  constructor(hubClient: HubRpcClient, db: DB, log: Logger) {
    this.hubClient = hubClient;
    this.db = db;
    this.log = log;
  }

  async reconcileUsernameProofsForFid(
    fid: number,
    onHubProof: (proof: UserNameProof, missingInDb: boolean) => Promise<void>,
    onDbProof?: (proof: UserNameProof, missingInHub: boolean) => Promise<void>,
    startTimestamp?: number,
    stopTimestamp?: number,
    types?: UserNameType[],
  ) {
    let startDate: Date | undefined;
    if (startTimestamp) {
      const startUnixTimestampResult = fromFarcasterTime(startTimestamp);
      if (startUnixTimestampResult.isErr()) {
        this.log.error({ fid, types, startTimestamp, stopTimestamp }, "Invalid time range provided to reconciliation");
        return;
      }

      startDate = new Date(startUnixTimestampResult.value);
    }

    let stopDate: Date | undefined;
    if (stopTimestamp) {
      const stopUnixTimestampResult = fromFarcasterTime(stopTimestamp);
      if (stopUnixTimestampResult.isErr()) {
        this.log.error({ fid, types, startTimestamp, stopTimestamp }, "Invalid time range provided to reconciliation");
        return;
      }

      stopDate = new Date(stopUnixTimestampResult.value);
    }

    for (const proofType of types ?? [UserNameType.USERNAME_TYPE_FNAME, UserNameType.USERNAME_TYPE_ENS_L1]) {
      this.log.debug({ fid, proofType, startTimestamp, stopTimestamp }, "Reconciling username proofs for FID");
      await this.reconcileUsernameProofsOfTypeForFid(fid, proofType, onHubProof, onDbProof, startDate, stopDate);
    }
  }

  async reconcileUsernameProofsOfTypeForFid(
    fid: number,
    proofType: UserNameType,
    onHubProof: (proof: UserNameProof, missingInDb: boolean) => Promise<void>,
    onDbProof?: (proof: UserNameProof, missingInHub: boolean) => Promise<void>,
    startDate?: Date,
    stopDate?: Date,
  ): Promise<void> {
    const hubProofsResult = await this.getProofsFromHub(fid, proofType, startDate, stopDate);
    if (hubProofsResult.isErr()) {
      throw hubProofsResult.error;
    }
    const hubProofs = hubProofsResult.value;

    if (hubProofs.length === 0) {
      this.log.debug(
        { fid, proofType, startDate: startDate?.toISOString(), stopDate: stopDate?.toISOString() },
        "No username proofs found in hub",
      );
      return;
    }

    const dbProofs = await this.getProofsFromDb(fid, proofType, startDate, stopDate);
    if (dbProofs.isErr()) {
      throw dbProofs.error;
    }

    // Track latest hub proofs for DB reconciliation
    const hubProofsByKey = new Map<string, UserNameProof>();

    // First process hub proofs and build map for later
    const dbProofsByKey = new Map<string, UserNameProof>();
    for (const proof of dbProofs.value) {
      const key = this.getProofKey(proof);
      const existingProof = dbProofsByKey.get(key);
      if (!existingProof || (existingProof.timestamp && proof.timestamp > existingProof.timestamp)) {
        dbProofsByKey.set(key, proof);
      }
    }

    // Process hub proofs immediately and track for DB reconciliation
    for (const proof of hubProofs) {
      const key = this.getProofKey(proof);
      hubProofsByKey.set(key, proof);
      const dbProof = dbProofsByKey.get(key);
      const missingInDb = !dbProof;
      await onHubProof(proof, missingInDb);
    }

    // Reconcile DB proofs that might be missing from hub
    if (onDbProof) {
      for (const [key, dbProof] of dbProofsByKey) {
        const missingInHub = !hubProofsByKey.has(key);
        await onDbProof(dbProof, missingInHub);
      }
    }
  }

  private async getProofsFromHub(
    fid: number,
    proofType?: UserNameType,
    startDate?: Date,
    stopDate?: Date,
  ): Promise<Result<UserNameProof[], Error>> {
    const result = await this.hubClient.getUserNameProofsByFid({ fid });
    if (result.isErr()) {
      return err(new Error(`Unable to get username proofs for FID ${fid}`, { cause: result.error }));
    }

    let proofs = result.value.proofs;

    if (proofType !== undefined) {
      proofs = proofs.filter((proof) => proof.type === proofType);
    }

    if (startDate || stopDate) {
      proofs = proofs.filter((proof) => {
        if (startDate !== undefined && proof.timestamp * 1000 < startDate.getTime()) return false;
        if (stopDate !== undefined && proof.timestamp * 1000 > stopDate.getTime()) return false;
        return true;
      });
    }

    return ok(proofs);
  }

  private async getProofsFromDb(
    fid: number,
    proofType?: UserNameType,
    startDate?: Date,
    stopDate?: Date,
  ): Promise<Result<UserNameProof[], Error>> {
    try {
      let query = this.db
        .selectFrom("usernames")
        .select(["username", "fid", "proofTimestamp", "custodyAddress"])
        .where("fid", "=", fid)
        .where("deletedAt", "is", null);

      if (startDate) {
        query = query.where("proofTimestamp", ">", startDate);
      }
      if (stopDate) {
        query = query.where("proofTimestamp", "<", stopDate);
      }

      const results = await query.execute();

      const proofs = results.map((row) => ({
        name: Buffer.from(row.username),
        type: UserNameType.USERNAME_TYPE_FNAME,
        fid: row.fid,
        timestamp: Math.floor(row.proofTimestamp.getTime() / 1000),
        signature: Buffer.from([]),
        owner: row.custodyAddress ? Buffer.from(row.custodyAddress) : Buffer.from([]),
      }));

      const filteredProofs = proofType !== undefined ? proofs.filter((p) => p.type === proofType) : proofs;
      return ok(filteredProofs);
    } catch (e) {
      return err(new Error(`Failed to get username proofs from DB for FID ${fid}`, { cause: e }));
    }
  }

  private getProofKey(proof: UserNameProof): string {
    // Only include identifying fields (name, owner, fid, type) - not timestamp
    const nameHex = bytesToHexString(proof.name)._unsafeUnwrap();
    const ownerHex = bytesToHexString(proof.owner)._unsafeUnwrap();
    return `${nameHex}-${proof.fid}-${ownerHex}-${proof.type}`;
  }
}
