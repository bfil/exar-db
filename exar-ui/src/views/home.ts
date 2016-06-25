import {autoinject} from 'aurelia-framework';

import * as $ from 'jquery';

import {ExarClient} from '../exar/client';
import {Connection, Event, Query} from '../exar/model';
import {TcpMessage} from '../exar/net';

import {SavedConnection} from '../models/saved-connection';

@autoinject
export class Home {
    tabs: Tab[] = [];
    savedConnections: SavedConnection[];

    constructor() {
        this.savedConnections = localStorage.getItem('connections.saved') ? JSON.parse(localStorage.getItem('connections.saved')) : [];
    }

    addTab() {
        this.tabs.push(new Tab(this.savedConnections));
        setTimeout(() => $(`#tab-${this.tabs.length - 1}`).tab('show'));
    }

    removeTab(index) {
        this.disconnect(this.tabs[index]);
        this.tabs.splice(index, 1);
    }

    connect(tab: Tab) {
        tab.exarClient = new ExarClient();
        tab.exarClient.connect(tab.initializeConnection(tab.collection, tab.selectedConnection))
            .then(connected => {
                tab.connected = true;
                tab.logTcpMessage(connected);
            }, tab.onError.bind(tab));
        tab.exarClient.onDisconnect(() => {
            tab.connected = false;
            tab.logMessage(`Disconnected`);
        });
    }

    publish(tab: Tab) {
        let event = new Event(tab.data, (tab.tags || '').split(' '));
        tab.exarClient.publish(event).then(
            published => tab.logTcpMessage(published),
            tab.onError.bind(tab)
        )
    }

    subscribe(tab: Tab) {
        let query = new Query(false, parseInt(tab.offset), parseInt(tab.limit), tab.tag);
        tab.exarClient.subscribe(query).then(
            eventStream => {
                tab.logMessage('Subscribed');
                eventStream.subscribe(
                    tab.logTcpMessage.bind(tab),
                    tab.onError.bind(tab),
                    () => tab.logMessage('EndOfEventStream')
                );
            },
            tab.onError.bind(tab)
        )
    }

    disconnect(tab: Tab) {
        tab.exarClient.disconnect();
    }

    selectConnection(tab: Tab, connection: SavedConnection) {
        tab.selectedConnection = connection;
    }
}

export class Tab {
    exarClient: ExarClient;

    selectedConnection: SavedConnection;
    collection: string;

    data: string;
    tags: string;

    liveStream: boolean;
    offset: string;
    limit: string;
    tag: string;

    connected: boolean;
    messages: string[];

    constructor(connections: SavedConnection[]) {
        if(connections.length) {
            this.selectedConnection = connections[0];
        }
        this.connected = false;
        this.messages = [];
    }

    get name() {
        if(this.collection) {
            return `${this.collection} @ ${this.selectedConnection.alias}`;
        } else {
            return 'New connection';
        }
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
