import {Connection, Event, Query} from './model';
import {Connect, Connected, Publish, Published, Subscribe, Subscribed, DatabaseError, TcpMessage} from './net';

import * as Rx from 'rx';

export class ExarClient {
    
    private socket: TCPSocket;
    private socketObservable: Rx.ControlledObservable<string>;
    
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
    
    private requestSubscription: Rx.IDisposable;
    private request<T>(message: TcpMessage, handleResponse: (message: string) => T, sendOnOpen: boolean = false) {
        return new Promise<T>((resolve, reject) => {
            if(this.requestSubscription) this.requestSubscription.dispose();
            this.requestSubscription = this.socketObservable.subscribe(message => {
                resolve(handleResponse(message));
            }, reject);
            if(sendOnOpen) this.socket.onopen = () => this.send(message);
            else this.send(message);
            this.socketObservable.request(1);
        });
    }
    
    private createSocketObservable() {
        this.socketObservable = Rx.Observable.create<string>(observer => {
            this.socket.ondata = message => {
                let messages = this.decode(message.data).split('\n').filter(m => !!m);
                for(let message of messages) {
                    if (message.startsWith('Error')) {
                        observer.onError(DatabaseError.fromTabSeparatedString(message));
                        this.createSocketObservable();
                    }
                    else if (message) observer.onNext(message);
                }
            };
            this.socket.onerror = error => observer.onError(error.data);
        }).controlled();
    }
    
    connect(connectionInfo: Connection) {
        this.socket = navigator.TCPSocket.open(connectionInfo.host, connectionInfo.port);
        this.createSocketObservable();
        return this.request(
            new Connect(connectionInfo.collection, connectionInfo.username, connectionInfo.password),
            Connected.fromTabSeparatedString, true);
    }
    
    onDisconnect(onDisconnect: () => any) {
        this.socket.onclose = onDisconnect;
    }
    
    disconnect() {
        this.socket.close();
    }
    
    publish(event: Event) {
        return this.request(new Publish(event), Published.fromTabSeparatedString);
    }
    
    subscribe(query: Query) {
        return this.request(new Subscribe(query), message => {
            return Rx.Observable.create<Event>(observer => {
                let subscription = this.socketObservable.subscribe(message => {
                    if (message === 'EndOfEventStream') {
                        observer.onCompleted();
                        subscription.dispose();
                    } else {
                        observer.onNext(Event.fromTabSeparatedString(message));
                        this.socketObservable.request(1);
                    }     
                });
                this.socketObservable.request(1);
            });
        });
    }
    
}