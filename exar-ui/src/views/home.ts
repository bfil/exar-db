import {autoinject} from 'aurelia-framework';

import * as $ from 'jquery';

import {ExarClient} from '../exar/client';
import {Event, Query} from '../exar/model';

@autoinject
export class Home {
    
    connections: Connection[] = [];
    
    exarClient: ExarClient;
    
    addConnection() {
        this.connections.push(new Connection());
        setTimeout(() => $(`#tab-${this.connections.length - 1}`).tab('show'));
    }
    
    removeConnection(index) {
        this.connections.splice(index, 1);
    }
    
    constructor() {
        this.exarClient = new ExarClient();
    }
    
    activate() {
        
    }
    
    connect(connection: Connection) {
        this.exarClient.connect(connection.collection, () => {
            connection.connected = false;
            connection.logMessage(`Disconnected`);
        })
        .then(connected => {
            connection.connected = true;
            connection.logMessage(connected);
        }, error => {
            connection.logMessage(`Error: ${error.message}`);
        })
    }
    
    publish(connection: Connection) {
        let event = new Event(connection.tags.split(' '), connection.data);
        this.exarClient.publish(event).then(
            published => connection.logMessage(published), 
            error => connection.logMessage(`Error: ${error.message}`)
        )
    }
    
    subscribe(connection: Connection) {
        let query = new Query(false, parseInt(connection.offset), parseInt(connection.limit), connection.tag);
        this.exarClient.subscribe(query, event => {
            connection.logMessage(event);
        }).then(
            subscribed => connection.logMessage(subscribed), 
            error => connection.logMessage(`Error: ${error.message}`)
        )
    }
    
    disconnect() {
        this.exarClient.disconnect();
    }
}

export class Connection {
    collection: string;
    
    data: string;
    tags: string;
    
    offset: string;
    limit: string;
    tag: string;
    
    connected: boolean;
    messages: string[];
    socket: TCPSocket;
    
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