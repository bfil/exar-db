import {autoinject} from 'aurelia-framework';

import {SavedConnection} from '../models/saved-connection';

@autoinject
export class Settings {
    savedConnections: SavedConnection[];
    editingConnection: SavedConnection;
    editing = false;

    constructor() {
        this.savedConnections = localStorage.getItem('connections.saved') ? JSON.parse(localStorage.getItem('connections.saved')) : [];
    }

    newConnection() {
        this.editingConnection = new SavedConnection();
        this.editing = true;
    }

    editConnection(connection) {
        this.editingConnection = connection;
        this.editing = true;
    }

    deleteConnection(connection) {
        let index = this.savedConnections.indexOf(connection);
        this.savedConnections.splice(index, 1);
        localStorage.setItem('connections.saved', JSON.stringify(this.savedConnections));
    }

    saveConnection() {
        if(this.savedConnections.indexOf(this.editingConnection) === -1) {
            this.savedConnections.push(this.editingConnection);
        }
        this.editing = false;
        localStorage.setItem('connections.saved', JSON.stringify(this.savedConnections));
    }

    cancelConnection() {
        this.editing = false;
    }
}
