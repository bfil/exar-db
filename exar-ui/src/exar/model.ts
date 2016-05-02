import {TcpMessage, TcpMessageEncoder, TcpMessageDecoder} from './net';

export class ConnectionInfo {
    host: string;
    port: number;
    username: string;
    password: string;
    collection: string;
    
    constructor(collection: string, info: { host?: string, port?: number, username?: string, password?: string }) {
        this.collection = collection;
        this.host = info.host || 'localhost';
        this.port = info.port || 38580;
        this.username = info.username;
        this.password = info.password;
    }
}

export class Event implements TcpMessage {
    id: number = 0;
    tags: string[];
    timestamp: number = 0;
    data: string;
    
    constructor(data: string, tags: string[]) {
        this.data = data;
        this.tags = tags;
    }
    
    withId(id: number) {
        this.id = id;
        return this;
    }
    
    withTimestamp(timestamp: number) {
        this.timestamp = timestamp;
        return this;
    }
    
    toTabSeparatedString(): string {
       return TcpMessageEncoder.toTabSeparatedString('Event',
           this.id || 0,
           this.timestamp || 0,
           this.tags.join(' '),
           this.data);
    }
    
    static fromTabSeparatedString(data: string): Event {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 5);
        let id = parseInt(messageParts[1]);
        let timestamp = parseInt(messageParts[2]);
        let tags = messageParts[3].split(' ');
        let eventData = messageParts[4];
        return new Event(eventData, tags).withId(id).withTimestamp(timestamp);
    }
}

export class Query {
    liveStream: boolean;
    offset: number;
    limit: number;
    tag: string;
    
    constructor(liveStream: boolean, offset: number = 0, limit: number = 0, tag?: string) {
        this.liveStream = liveStream;
        this.offset = offset;
        this.limit = limit;
        this.tag = tag;
    }
}