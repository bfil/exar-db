import {Event, Query} from 'exar/model';

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

export class Authenticate implements TcpMessage {

    private username: string;
    private password: string;

    constructor(username: string, password: string) {
        this.username = username;
        this.password = password;
    }

    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Authenticate', this.username, this.password);
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 3);
        let username = messageParts[1];
        let password = messageParts[2];
        return new Authenticate(username, password);
    }
}

export class Authenticated implements TcpMessage {

    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Authenticated');
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 1);
        return new Authenticated();
    }
}

export class Select implements TcpMessage {

    private collection: string;

    constructor(collection: string) {
        this.collection = collection;
    }

    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Select', this.collection);
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 2);
        let collection = messageParts[1];
        return new Select(collection);
    }
}

export class Selected implements TcpMessage {

    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Selected');
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 1);
        return new Selected();
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

export class Unsubscribe implements TcpMessage {

    toTabSeparatedString() {
        return TcpMessageEncoder.toTabSeparatedString('Unsubscribe');
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 1);
        return new Unsubscribe();
    }

}

export class EndOfEventStream implements TcpMessage {

    toTabSeparatedString() {
        return TcpMessageEncoder.toTabSeparatedString('EndOfEventStream');
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 1);
        return new EndOfEventStream();
    }

}

export class Drop implements TcpMessage {

    private collection: string;

    constructor(collection: string) {
        this.collection = collection;
    }

    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Drop', this.collection);
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 2);
        let collection = messageParts[1];
        return new Drop(collection);
    }
}

export class Dropped implements TcpMessage {

    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Dropped');
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 1);
        return new Dropped();
    }
}

export class DatabaseError implements TcpMessage {

    private type: string;
    private subType: string;
    private data: string;

    constructor(type: string, data: string, subType?: string) {
        this.type = type;
        this.subType = subType;
        this.data = data;
    }

    toTabSeparatedString() {
       return TcpMessageEncoder.toTabSeparatedString('Error', this.type, this.subType, this.data);
    }

    toString() {
        if(this.type === 'ParseError' && this.subType === 'MissingField') {
            return `${this.type}: missing field at position ${this.data}`;
        } else if(this.type === 'AuthenticationError') {
            return `${this.type}: missing or invalid credentials`;
        } else if(this.data) {
          return `${this.type}: ${this.data}`;
        } else {
          return `${this.type}`;
        }
    }

    static fromTabSeparatedString(data: string) {
        let messageParts = TcpMessageDecoder.parseTabSeparatedString(data, 4);
        let errorData = messageParts[3] || messageParts[2];
        let subType = messageParts[3] ? messageParts[2] : undefined;
        return new DatabaseError(messageParts[1], errorData, subType);
    }
}
