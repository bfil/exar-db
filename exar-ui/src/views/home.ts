import {autoinject} from 'aurelia-framework';

interface Connection {
    collection: string;
    
    data?: string;
    tags?: string;
    
    offset?: string;
    limit?: string;
    tag?: string;
    
    connected: boolean;
    messages: string[];
    socket?: TCPSocket;
    
    logMessage(message: String);
}

class Connection {
    
    constructor() {
        this.collection = '';
        this.connected = false;
        this.messages = [];
        this.socket = undefined;
    }
    
    logMessage(message: string) {
        this.messages.push(message);
    }
    clearMessages() {
        this.messages = [];
    }
}

@autoinject
export class Home {
    
    encoder: TextEncoding.TextEncoder;
    decoder: TextEncoding.TextDecoder;
    
    connections: Connection[] = [];
    
    addConnection() {
        this.connections.push(new Connection());
    }
    
    removeConnection(index) {
        this.connections.splice(index, 1);
    }
    
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
    
    connect(connection: Connection) {
        connection.socket = navigator.TCPSocket.open("localhost", 38580);
        connection.socket.onopen = () => {
            connection.socket.send(this.encode(`Connect\t${connection.collection}\tadmin\tsecret\n`));
        };
        
        connection.socket.ondata = this.awaitingConnection(connection).bind(this);
        
        connection.socket.onerror = event => {
            let error = event.data;
            connection.logMessage(`Error: ${error.message}`);
        };
        
        connection.socket.onclose = () => {
            connection.connected = false;
            connection.logMessage("Disconnected");
        };
    }
    
    awaitingConnection(connection: Connection) {
        return event => {
            connection.connected = true;
            connection.logMessage(this.decode(event.data));
        };
    }
    
    awaitingPublished(connection: Connection) {
        return event => {
            connection.logMessage(this.decode(event.data));
        };
    }
    
    awaitingEvents(connection: Connection) {
        return event => {
            let events = this.decode(event.data).split('\n');
            for(event of events) {
                if(event) connection.logMessage(event);    
            }
        }
    }
    
    disconnect(connection: Connection) {
        connection.socket.close();
    }
    
    publish(connection: Connection) {
        connection.socket.ondata = this.awaitingPublished(connection).bind(this);
        connection.socket.send(this.encode(`Publish\t${connection.tags || ''}\t0\t${connection.data || ''}\n`));
    }
    
    subscribe(connection: Connection) {
        connection.socket.ondata = this.awaitingEvents(connection).bind(this);
        let optionalTag = connection.tag ? `\t${connection.tag}`: '';
        connection.socket.send(this.encode(`Subscribe\tfalse\t${connection.offset || 0}\t${connection.limit || 0}${optionalTag}\n`));
    }
}