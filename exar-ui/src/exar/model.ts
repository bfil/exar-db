export class Event {
    tags: string[];
    data: string;
    
    constructor(tags: string[], data: string) {
        this.tags = tags;
        this.data = data;
    }
}

export class Query {
    live_stream: boolean;
    offset: number;
    limit: number;
    tag: string;
    
    constructor(live_stream: boolean, offset: number, limit: number, tag: string) {
        this.live_stream = live_stream;
        this.offset = offset;
        this.limit = limit;
        this.tag = tag;
    }
}