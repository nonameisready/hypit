import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";

import { DeleteObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

import type { WorkerConfig } from "../config.js";
import { withRetry, type RetryOptions } from "../retry.js";

export interface ObjectStorage {
  putFile(key: string, path: string, contentType: string, timeoutMs: number): Promise<number>;
  putBytes(key: string, bytes: Uint8Array, contentType: string, timeoutMs: number): Promise<number>;
  listObjects(prefix: string): Promise<readonly ObjectInfo[]>;
  deleteObject(key: string): Promise<void>;
}

export type ObjectInfo = {
  readonly key: string;
  readonly size: number;
  readonly lastModified?: string;
};

export class R2Storage implements ObjectStorage {
  readonly #client: S3Client;
  readonly #bucket: string;
  readonly #retry: RetryOptions;

  constructor(config: WorkerConfig, retry: RetryOptions, client?: S3Client) {
    if (config.r2.accessKeyId === undefined || config.r2.secretAccessKey === undefined || config.r2.bucket === undefined || config.r2.endpoint === undefined) {
      throw new Error("R2Storage requires R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, and R2_ENDPOINT");
    }
    this.#bucket = config.r2.bucket;
    this.#retry = retry;
    this.#client = client ?? new S3Client({
      region: "auto",
      endpoint: config.r2.endpoint,
      credentials: { accessKeyId: config.r2.accessKeyId, secretAccessKey: config.r2.secretAccessKey },
    });
  }

  async putFile(key: string, path: string, contentType: string, timeoutMs: number): Promise<number> {
    const size = (await stat(path)).size;
    await withRetry(async () => {
      await this.#client.send(new PutObjectCommand({
        Bucket: this.#bucket,
        Key: key,
        Body: createReadStream(path),
        ContentLength: size,
        ContentType: contentType,
      }), { abortSignal: AbortSignal.timeout(timeoutMs) });
    }, this.#retry);
    return size;
  }

  async putBytes(key: string, bytes: Uint8Array, contentType: string, timeoutMs: number): Promise<number> {
    await withRetry(async () => {
      await this.#client.send(new PutObjectCommand({
        Bucket: this.#bucket,
        Key: key,
        Body: bytes,
        ContentLength: bytes.byteLength,
        ContentType: contentType,
      }), { abortSignal: AbortSignal.timeout(timeoutMs) });
    }, this.#retry);
    return bytes.byteLength;
  }

  async listObjects(prefix: string): Promise<readonly ObjectInfo[]> {
    const objects: ObjectInfo[] = [];
    let continuationToken: string | undefined;
    do {
      const response = await withRetry(() => this.#client.send(new ListObjectsV2Command({
        Bucket: this.#bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })), this.#retry);
      for (const object of response.Contents ?? []) {
        if (object.Key === undefined) continue;
        objects.push({ key: object.Key, size: object.Size ?? 0, ...(object.LastModified === undefined ? {} : { lastModified: object.LastModified.toISOString() }) });
      }
      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken !== undefined);
    return objects;
  }

  async deleteObject(key: string): Promise<void> {
    await withRetry(async () => {
      await this.#client.send(new DeleteObjectCommand({ Bucket: this.#bucket, Key: key }));
    }, this.#retry);
  }
}
