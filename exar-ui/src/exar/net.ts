import {Event, Query} from './model';

export class TcpMessageEncoder {
    static toTabSeparatedString(...args): string {
        return args.filter(arg => typeof arg !== 'undefined').join('\t') + '\n';
    }
}

export class TcpMessageDecoder {
    static parseTabSeparatedString(data: string, numberOfParts: number): string[] {
        return data.split('\t', numberOfParts);
    }
}

export interface TcpMessage {
    toTabSeparatedString(): string;
}

export class Connect implements TcpMessage {
    
    private collection: string;
    private username: string;
    private password: string;
    
    constructor(collection: string, username?: string, password?: string) {
        this.collection = collection;
        this.username = username;
        this.password = password;
    }
    
    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Connect',
           this.collection,
           this.username,
           this.password);
    }
    
    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 4);
        let collection = messageParts[1];
        let username = messageParts[2];
        let password = messageParts[3];
        return new Connect(collection, username, password);
    }
}

export class Connected implements TcpMessage {
    
    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Connected'); 
    }
    
    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 1);
        return new Connected();
    }
}

export class Publish implements TcpMessage {
    
    private event: Event;
    
    constructor(event: Event) {
        this.event = event;
    }
    
    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Publish',
           this.event.tags.join(' '),
           this.event.timestamp,
           this.event.data);
    }
    
    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 4);
        let tags = messageParts[1].split(' ');
        let timestamp = parseInt(messageParts[2]);
        let eventData = messageParts[3];
        let event = new Event(eventData, tags).withTimestamp(timestamp);
        return new Publish(event);
    }
}

export class Published implements TcpMessage {
    
    private eventId: number;
    
    constructor(eventId: number) {
        this.eventId = eventId;
    }
    
    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Published', this.eventId); 
    }
    
    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 2);
        let eventId = parseInt(messageParts[1]);
        return new Published(eventId);
    }
}

export class Subscribe implements TcpMessage {
    
    private query: Query;
    
    constructor(query: Query) {
        this.query = query;
    }
    
    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Subscribe',
           this.query.liveStream,
           this.query.offset || 0,
           this.query.limit || 0,
           this.query.tag);
    }
    
    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 5);
        let liveStream = messageParts[1] === 'true';
        let offset = parseInt(messageParts[2]);
        let limit = parseInt(messageParts[3]);
        let tag = messageParts[4];
        let query = new Query(liveStream, offset, limit, tag);
        return new Subscribe(query);
    }
}

export class Subscribed implements TcpMessage {
    
    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Subscribed'); 
    }
    
    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 1);
        return new Subscribed();
    }
}