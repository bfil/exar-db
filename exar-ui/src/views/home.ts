import {autoinject} from 'aurelia-framework';

import * as $ from 'jquery';

import {SavedConnection} from 'models/saved-connection';

@autoinject
export class Home {
    tabs: Tab[] = [];

    addTab() {
        this.tabs.push(new Tab());
        this.showTab(this.tabs.length - 1);
    }

    removeTab(index, event) {
        this.tabs.splice(index, 1);
        event.stopPropagation();
        let selectedTabIndex = this.getSelectedTabIndex();
        if(index > selectedTabIndex) this.showTab(selectedTabIndex);
        else if(index < selectedTabIndex) this.showTab(selectedTabIndex - 1);
    }

    showTab(index) {
        setTimeout(() => $(`#tab-${index}`).tab('show'));
    }

    getSelectedTabIndex() {
        return Number($('.tab-pane.active').attr('id').replace('tab-content-', ''));
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
