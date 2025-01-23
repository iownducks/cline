import { MessageStorage } from '../utils/messageStorage';
import * as vscode from 'vscode';
import * as path from 'path';

export class ClineMessageManager {
    private storage: MessageStorage;
    private context: vscode.ExtensionContext;

    constructor(context: vscode.ExtensionContext) {
        this.context = context;
        const storagePath = path.join(context.globalStorageUri.fsPath, 'cline_messages');
        this.storage = new MessageStorage(storagePath);
    }

    async saveClineMessages(messages: any[]): Promise<void> {
        try {
            await this.storage.saveMessages(messages);
        } catch (error) {
            // Log error but don't throw to prevent UI disruption
            console.error('Failed to save Cline messages:', error);
            // Show error notification only for persistent failures
            if (error.message.includes('MAX_RETRIES')) {
                vscode.window.showErrorMessage(
                    'Failed to save conversation history. Some messages may be lost.'
                );
            }
        }
    }

    async loadClineMessages(): Promise<any[]> {
        try {
            return await this.storage.loadMessages();
        } catch (error) {
            console.error('Failed to load Cline messages:', error);
            return []; // Return empty array on failure
        }
    }

    async cleanup(): Promise<void> {
        await this.storage.cleanup();
    }
}
