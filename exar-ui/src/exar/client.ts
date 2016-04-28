import {Event, Query} from './model';
import {Connect, Connected, Publish, Published, Subscribe, Subscribed, TcpMessage} from './net';

import * as Rx from 'rx';

export class ExarClient {
    
    private socket: TCPSocket;
    private socketObservable: Rx.Observable<string>;
    
    private encoder: TextEncoding.TextEncoder;
    private decoder: TextEncoding.TextDecoder;
    
    constructor() {
        this.encoder = new TextEncoder("utf8");
        this.decoder = new TextDecoder("utf8");
    }
    
    private encode(data: string): ArrayBufferView {
        return this.encoder.encode(data);
    }
    
    private decode(data: ArrayBufferView): string {
        return this.decoder.decode(data);
    }
    
    private send(message: TcpMessage) {
        this.socket.send(this.encode(message.toTabSeparatedString()));
    }
    
    private createSocketObservable() {
        this.socketObservable = Rx.Observable.create<string>(observer => {
            this.socket.ondata = message => {
                let messages = this.decode(message.data).split('\n').filter(m => !!m);
                for(let message of messages) {
                    if(message.startsWith("Error")) observer.onError(new Error(message))
                    else if(message) observer.onNext(message);
                }    
            };
            this.socket.onerror = error => observer.onError(error.data);
        });
    }
    
    connect(collection: string) {
        return new Promise<Connected>((resolve, reject) => {
            this.socket = navigator.TCPSocket.open("localhost", 38580);
            this.socket.onopen = () => this.send(new Connect(collection, 'admin', 'secret'));
            this.createSocketObservable();
            
            let subscription = this.socketObservable.take(1).subscribe(message => {
                if(message.startsWith("Error")) reject(new Error(message))
                else resolve(Connected.fromTabSeparatedString(message));
                subscription.dispose();
            }, reject);
        })
    }
    
    onClose(onClose: () => any) {
        this.socket.onclose = onClose;
    }
    
    disconnect() {
        this.socket.close();
    }
    
    publish(event: Event) {
        return new Promise<Published>((resolve, reject) => {
            let subscription = this.socketObservable.take(1).subscribe(message => {
                if(message.startsWith("Error")) reject(new Error(message))
                else resolve(Published.fromTabSeparatedString(message))
                subscription.dispose();
            }, reject);
            this.send(new Publish(event));
        });
    }
    
    subscribe(query: Query) {
        return new Promise<Rx.Observable<Event>>((resolve, reject) => {
            
            let subscription = this.socketObservable.take(1).subscribe(message => {
                 resolve(observable);
                 subscription.dispose();
             }, reject);
            this.send(new Subscribe(query));
            
            let observable = Rx.Observable.create<Event>(observer => {
                let subscription = this.socketObservable.subscribe(message => {
                    if(message.startsWith("Error")) {
                        observer.onError(new Error(message));
                        subscription.dispose();
                    } else if(message === 'EndOfEventStream') {
                        observer.onCompleted();
                        subscription.dispose();
                    } else if(message) observer.onNext(Event.fromTabSeparatedString(message));     
                });
            }); 
                       
        });
    }
    
}