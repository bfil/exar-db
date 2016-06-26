import {autoinject, bindable} from 'aurelia-framework';

import {ExarClient} from 'exar/client';
import {Connection, Event, Query} from 'exar/model';
import {TcpMessage} from 'exar/net';

import {SavedConnection} from 'models/saved-connection';

@autoinject
export class ConnectionHandler {
    exarClient: ExarClient;

    savedConnections: SavedConnection[];
    @bindable connection: SavedConnection;
    @bindable collection: string;

    data: string;
    tags: string;

    liveStream: boolean;
    offset: string;
    limit: string;
    tag: string;

    connected: boolean;
    messages: string[];

    bind() {
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

    connect() {
        this.exarClient = new ExarClient();
        this.exarClient.connect(this.initializeConnection(this.collection, this.connection))
            .then(connected => {
                this.connected = true;
                this.logTcpMessage(connected);
            }, this.onError.bind(this));
        this.exarClient.onDisconnect(() => {
            this.connected = false;
            this.logMessage(`Disconnected`);
        });
    }

    publish() {
        let event = new Event(this.data, (this.tags || '').split(' '));
        this.exarClient.publish(event).then(
            published => this.logTcpMessage(published),
            this.onError.bind(this)
        )
    }

    subscribe() {
        let query = new Query(false, parseInt(this.offset), parseInt(this.limit), this.tag);
        this.exarClient.subscribe(query).then(
            eventStream => {
                this.logMessage('Subscribed');
                eventStream.subscribe(
                    this.logTcpMessage.bind(this),
                    this.onError.bind(this),
                    () => this.logMessage('EndOfEventStream')
                );
            },
            this.onError.bind(this)
        )
    }

    disconnect() {
        this.exarClient.disconnect();
    }

    selectConnection(connection: SavedConnection) {
        this.connection = connection;
    }

    onError(error: any) {
        this.logMessage(error.toString());
    }

    logMessage(message: string) {
        this.messages.push(message);
    }

    logTcpMessage(message: TcpMessage) {
        this.logMessage(message.toTabSeparatedString());
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
