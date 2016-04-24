import {Event, Query} from './model';

export class ExarClient {
    
    private encoder: TextEncoding.TextEncoder;
    private decoder: TextEncoding.TextDecoder;
    
    private socket: TCPSocket;
    
    constructor() {
        this.encoder = new TextEncoder("utf8");
        this.decoder = new TextDecoder("utf8");
    }
    
    private encode(data: string) {
        return this.encoder.encode(data);
    }
    
    private decode(data: ArrayBufferView) {
        return this.decoder.decode(data);
    }
    
    connect(collection: string, onClose: () => any) {
        return new Promise<string>((resolve, reject) => {
            
            this.socket = navigator.TCPSocket.open("localhost", 38580);
            this.socket.onopen = () => {
                this.socket.send(this.encode(`Connect\t${collection}\tadmin\tsecret\n`));
            };
            
            this.socket.ondata = event => resolve(this.decode(event.data));
            
            this.socket.onerror = error => reject(error.data);
            
            this.socket.onclose = onClose;
        })
    }
    
    disconnect() {
        this.socket.close();
    }
    
    publish(event: Event) {
        return new Promise<string>((resolve, reject) => {
            this.socket.ondata = event => resolve(this.decode(event.data));
            this.socket.onerror = error => reject(error.data);
            
            this.socket.send(this.encode(`Publish\t${event.tags.join(' ') || ''}\t0\t${event.data || ''}\n`));
        });
    }
    
    subscribe(query: Query, onEvent: (e: string) => any) {
        return new Promise<string>((resolve, reject) => {
            this.socket.ondata = subscribed => {
                resolve(this.decode(subscribed.data));
                this.socket.ondata = event => {
                    let events = this.decode(event.data).split('\n');
                    for(event of events) {
                        if(event) onEvent(event);  
                    }
                }
            };
            
            this.socket.onerror = error => reject(error.data);

            let optionalTag = query.tag ? `\t${query.tag}`: '';
            this.socket.send(this.encode(`Subscribe\t${query.live_stream}\t${query.offset || 0}\t${query.limit || 0}${optionalTag}\n`));
        });
    }
    
}