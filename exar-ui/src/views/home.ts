import {autoinject} from 'aurelia-framework';

@autoinject
export class Home {
    
    encoder: TextEncoding.TextEncoder;
    decoder: TextEncoding.TextDecoder;
    
    collection: string;
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
            this.logMessage("Socket opened..");
            this.socket.send(this.encode(`Connect\t${this.collection}\tadmin\tsecret\n`));
        };
        
        this.socket.ondata = event => {
            this.logMessage(this.decode(event.data));
        };
        
        this.socket.onerror = event => {
            let error = event.data;
            this.logMessage(`An error occurred: ${error.message}..`);
        };
        
        this.socket.onclose = () => this.logMessage("Socket closed..");
    }
    
    publish() {
        this.socket.send(this.encode("Publish\telectron\t0\tsome data\n"));
    }
    
    logMessage(message: string) {
        this.messages.push(message);
    }
}