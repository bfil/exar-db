import {autoinject} from 'aurelia-framework';

import * as $ from 'jquery';

import {SavedConnection} from 'models/saved-connection';

@autoinject
export class Home {
    tabs: Tab[] = [];

    addTab() {
        this.tabs.push(new Tab());
        setTimeout(() => $(`#tab-${this.tabs.length - 1}`).tab('show'));
    }

    removeTab(index) {
        this.tabs.splice(index, 1);
    }
}

class Tab {
    collection: string;
    connection: SavedConnection;

    get name() {
        if(this.collection) {
            return `${this.collection} @ ${this.connection.alias}`;
        } else {
            return 'New connection';
        }
    }
}
