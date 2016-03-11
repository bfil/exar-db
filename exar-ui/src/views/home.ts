import {autoinject} from 'aurelia-framework';

@autoinject
export class Home {
    
    encoder: TextEncoding.TextEncoder;
    decoder: TextEncoding.TextDecoder;
    
    collection: string = "test";
    
    data: string;
    tags: string;
    
    offset: string;
    limit: string;
    tag: string;
    
    connected: boolean = false;
    messages: string[] = [];
    socket: TCPSocket;
    
    constructor() {
        this.encoder = new TextEncoder("utf8");
        this.decoder = new TextDecoder("utf8");
    }
    
    encode(data: string) {
        return this.encoder.encode(data);
    }
    
    decode(data: ArrayBufferView) {
        return this.decoder.decode(data);
    }
    
    activate() {
        
    }
    
    connect() {
        this.socket = navigator.TCPSocket.open("localhost", 38580);
        this.socket.onopen = () => {
            this.socket.send(this.encode(`Connect\t${this.collection}\tadmin\tsecret\n`));
        };
        
        this.socket.ondata = this.awaitingConnection.bind(this);
        
        this.socket.onerror = event => {
            let error = event.data;
            this.logMessage(`Error: ${error.message}`);
        };
        
        this.socket.onclose = () => {
            this.connected = false;
            this.logMessage("Disconnected");
        };
    }
    
    awaitingConnection(event) {
        this.connected = true;
        this.logMessage(this.decode(event.data));
    }
    
    awaitingPublished(event) {
        this.logMessage(this.decode(event.data));
    }
    
    awaitingEvents(event) {
        let events = this.decode(event.data).split('\n');
        for(event of events) {
            if(event) this.logMessage(event);    
        }
    }
    
    disconnect() {
        this.socket.close();
    }
    
    publish() {
        this.socket.ondata = this.awaitingPublished.bind(this);
        this.socket.send(this.encode(`Publish\t${this.tags || ''}\t0\t${this.data || ''}\n`));
    }
    
    subscribe() {
        this.socket.ondata = this.awaitingEvents.bind(this);
        let optionalTag = this.tag ? `\t${this.tag}`: '';
        this.socket.send(this.encode(`Subscribe\tfalse\t${this.offset || 0}\t${this.limit || 0}${optionalTag}\n`));
    }
    
    logMessage(message: string) {
        this.messages.push(message);
    }
    
    clearMessages() {
        this.messages = [];
    }
}