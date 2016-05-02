import {autoinject} from 'aurelia-framework';

import * as $ from 'jquery';

import {ExarClient} from '../exar/client';
import {Connection, Event, Query} from '../exar/model';
import {TcpMessage} from '../exar/net';

@autoinject
export class Home {
    
    tabs: Tab[] = [];
    savedConnections: SavedConnection[];
    
    exarClient: ExarClient;
    
    addTab() {
        this.tabs.push(new Tab(this.savedConnections));
        setTimeout(() => $(`#tab-${this.tabs.length - 1}`).tab('show'));
    }
    
    removeTab(index) {
        this.tabs.splice(index, 1);
    }
    
    constructor() {
        this.exarClient = new ExarClient();
        this.savedConnections = localStorage.getItem('connections.saved') ? JSON.parse(localStorage.getItem('connections.saved')) : [];
    }
    
    activate() {
        
    }
    
    connect(tab: Tab) {
        this.exarClient.connect(tab.initializeConnection(tab.collection, tab.selectedConnection))
            .then(connected => {
                tab.connected = true;
                tab.logTcpMessage(connected);
            }, tab.onError.bind(tab));
        this.exarClient.onDisconnect(() => {
            tab.connected = false;
            tab.logMessage(`Disconnected`);
        });
    }
    
    publish(tab: Tab) {
        let event = new Event(tab.data, (tab.tags || '').split(' '));
        this.exarClient.publish(event).then(
            published => tab.logTcpMessage(published), 
            tab.onError.bind(tab)
        )
    }
    
    subscribe(tab: Tab) {
        let query = new Query(false, parseInt(tab.offset), parseInt(tab.limit), tab.tag);
        this.exarClient.subscribe(query).then(
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
    
    disconnect() {
        this.exarClient.disconnect();
    }
    
    newConnection(tab: Tab) {
        tab.editingConnection = new SavedConnection();
        tab.editing = true;
    }
    
    editConnection(tab: Tab) {
        tab.editingConnection = tab.selectedConnection;
        tab.editing = true;
    }
    
    deleteConnection(tab: Tab) {
        let index = this.savedConnections.indexOf(tab.selectedConnection);
        this.savedConnections.splice(index, 1);
        for(let tab of this.tabs) {
            if(this.savedConnections.length) {
                tab.selectedConnection = this.savedConnections[0];
            } else {
                tab.editingConnection = new SavedConnection();
                tab.selectedConnection = tab.editingConnection;
            }
        }
        localStorage.setItem('connections.saved', JSON.stringify(this.savedConnections));
    }
    
    saveConnection(tab: Tab) {
        if(this.savedConnections.indexOf(tab.editingConnection) === -1) {
            this.savedConnections.push(tab.editingConnection);
        }
        if(!tab.selectedConnection) tab.selectedConnection = tab.editingConnection;
        tab.editing = false;
        localStorage.setItem('connections.saved', JSON.stringify(this.savedConnections));
    }
    
    cancelConnection(tab: Tab) {
        tab.editing = false;
    }
    
    selectConnection(tab: Tab, connection: SavedConnection) {
        tab.selectedConnection = connection;
    }
}

export class SavedConnection {
    alias: string;
    requiresAuth: boolean;
    host: string = 'localhost';
    port: number = 38580;
    username: string;
    password: string;
    
    constructor() {
        
    }
}

export class Tab {
    editing: boolean;
    editingConnection: SavedConnection;
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
        } else {
            this.editing = true;
            this.editingConnection = new SavedConnection();
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