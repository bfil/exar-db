import {autoinject, bindable} from 'aurelia-framework';

import {ExarClient} from 'exar/client';
import {Connection, Event, Query} from 'exar/model';
import {EndOfEventStream, TcpMessage} from 'exar/net';

import {SavedConnection} from 'models/saved-connection';

import * as Rx from 'rx';

@autoinject
export class ConnectionHandler {
    exarClient: ExarClient;

    savedConnections: SavedConnection[];
    @bindable connection: SavedConnection;
    @bindable collection: string;

    data: string;
    tags: string;

    liveStream: boolean = false;
    offset: string;
    limit: string;
    tag: string;

    connected: boolean;
    disconnecting: boolean = false;
    unsubscribing: boolean = false;
    subscription: Rx.IDisposable;
    subscribed: boolean = false;
    messages: { payload: string, className: string }[];

    bind() {
        this.exarClient = new ExarClient();
        this.savedConnections = localStorage.getItem('connections.saved') ? JSON.parse(localStorage.getItem('connections.saved')) : [];
        if(this.savedConnections.length) {
            this.connection = this.savedConnections[0];
        }
        this.connected = false;
        this.messages = [];
    }

    unbind() {
        if(this.exarClient) this.disconnect();
    }

    connect(isReconnection: boolean = false) {
        this.exarClient.connect(this.initializeConnection(this.collection, this.connection))
            .then(connected => {
                this.connected = true;
                if(!isReconnection) this.logTcpMessage(connected);
            }, this.onError.bind(this));
        this.exarClient.onDisconnect(() => {
            this.disconnecting = false;
            this.connected = false;
            this.logMessage(`Disconnected`, false);
        });
    }

    disconnect() {
        this.disconnecting = true;
        this.exarClient.disconnect();
    }

    publish() {
        let event = new Event(this.data, (this.tags || '').split(' '));
        this.exarClient.publish(event).then(
            published => this.logTcpMessage(published),
            this.onError.bind(this)
        )
    }

    subscribe() {
        let query = new Query(this.liveStream, parseInt(this.offset), parseInt(this.limit), this.tag);
        this.exarClient.subscribe(query).then(
            eventStream => {
                this.logMessage('Subscribed', false);
                this.subscribed = true;
                this.subscription = eventStream.subscribe(
                    this.logTcpMessage.bind(this),
                    this.onError.bind(this),
                    () => {
                        this.logTcpMessage(new EndOfEventStream());
                        this.subscription = undefined;
                        this.unsubscribing = false;
                        this.subscribed = false;
                    }
                );
            },
            this.onError.bind(this)
        )
    }

    unsubscribe() {
        if(this.subscription) {
            this.unsubscribing = true;
            this.exarClient.unsubscribe();
        }
    }

    selectConnection(connection: SavedConnection) {
        this.connection = connection;
    }

    onError(error: any) {
        this.logMessage(error.toString(), true);
        this.unsubscribe();
    }

    logMessage(message: string, isError: boolean) {
        this.messages.push({
            payload: message,
            className: isError ? 'text-danger' : ''
        });
    }

    logTcpMessage(message: TcpMessage) {
        this.logMessage(message.toTabSeparatedString(), false);
    }

    clearMessages() {
        this.messages = [];
    }

    initializeConnection(collection: string, connection: SavedConnection): Connection {
        let username = connection.requiresAuth ? connection.username : undefined;
        let password = connection.requiresAuth ? connection.password : undefined;
        return new Connection(collection, connection.host, connection.port, username, password);
    }
}
