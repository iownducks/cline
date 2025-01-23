import { promises as fs } from 'fs';
import { createGzip, createGunzip } from 'zlib';
import { promisify } from 'util';
import { pipeline } from 'stream';

const pipelineAsync = promisify(pipeline);

const CHUNK_SIZE = 1024 * 1024; // 1MB chunks
const MAX_RETRIES = 3;

export class MessageStorage {
    private storagePath: string;

    constructor(storagePath: string) {
        this.storagePath = storagePath;
    }

    async saveMessages(messages: any[]): Promise<void> {
        let attempts = 0;
        while (attempts < MAX_RETRIES) {
            try {
                // Try compressed storage first
                await this.saveCompressed(messages);
                return;
            } catch (error) {
                attempts++;
                if (attempts === MAX_RETRIES) {
                    // Fall back to chunked storage on final attempt
                    await this.saveChunked(messages);
                }
            }
        }
    }

    private async saveCompressed(messages: any[]): Promise<void> {
        const messageStr = JSON.stringify(messages);
        const gzip = createGzip();
        const writeStream = fs.createWriteStream(`${this.storagePath}.gz`);
        
        try {
            await pipelineAsync(
                Buffer.from(messageStr),
                gzip,
                writeStream
            );
        } catch (error) {
            throw new Error(`Failed to save compressed messages: ${error.message}`);
        }
    }

    private async saveChunked(messages: any[]): Promise<void> {
        const messageStr = JSON.stringify(messages);
        const chunks: string[] = [];
        
        // Split into chunks
        for (let i = 0; i < messageStr.length; i += CHUNK_SIZE) {
            chunks.push(messageStr.slice(i, i + CHUNK_SIZE));
        }

        // Save chunk metadata
        await fs.writeFile(
            `${this.storagePath}.meta`,
            JSON.stringify({ totalChunks: chunks.length })
        );

        // Save each chunk
        await Promise.all(
            chunks.map((chunk, index) =>
                fs.writeFile(`${this.storagePath}.${index}`, chunk)
            )
        );
    }

    async loadMessages(): Promise<any[]> {
        try {
            // Try loading compressed file first
            return await this.loadCompressed();
        } catch (error) {
            // Fall back to loading chunks
            return await this.loadChunked();
        }
    }

    private async loadCompressed(): Promise<any[]> {
        const gunzip = createGunzip();
        const readStream = fs.createReadStream(`${this.storagePath}.gz`);
        let data = '';

        await pipelineAsync(
            readStream,
            gunzip,
            async function*(source) {
                for await (const chunk of source) {
                    data += chunk;
                }
            }
        );

        return JSON.parse(data);
    }

    private async loadChunked(): Promise<any[]> {
        // Load metadata
        const meta = JSON.parse(
            await fs.readFile(`${this.storagePath}.meta`, 'utf8')
        );

        // Load all chunks
        const chunks = await Promise.all(
            Array.from({ length: meta.totalChunks }, (_, i) =>
                fs.readFile(`${this.storagePath}.${i}`, 'utf8')
            )
        );

        // Combine chunks and parse
        return JSON.parse(chunks.join(''));
    }

    async cleanup(): Promise<void> {
        try {
            // Remove compressed file if exists
            await fs.unlink(`${this.storagePath}.gz`).catch(() => {});

            // Remove chunk files if they exist
            const meta = await fs.readFile(`${this.storagePath}.meta`, 'utf8')
                .catch(() => null);

            if (meta) {
                const { totalChunks } = JSON.parse(meta);
                await Promise.all(
                    Array.from({ length: totalChunks }, (_, i) =>
                        fs.unlink(`${this.storagePath}.${i}`).catch(() => {})
                    )
                );
                await fs.unlink(`${this.storagePath}.meta`).catch(() => {});
            }
        } catch (error) {
            console.error('Error during cleanup:', error);
        }
    }
}
