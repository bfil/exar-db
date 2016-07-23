import {autoinject} from 'aurelia-framework';
import {DialogService} from 'aurelia-dialog';

import {SavedConnection} from 'models/saved-connection';
import {EditConnection} from 'components/dialogs/edit-connection';

@autoinject
export class ManageConnections {
    savedConnections: SavedConnection[];
    dialogService: DialogService;

    constructor(dialogService: DialogService) {
        let storedConnections = localStorage.getItem('connections.saved');
        this.savedConnections = storedConnections ? JSON.parse(storedConnections) : [];
        this.dialogService = dialogService;
    }

    newConnection() {
        this.dialogService.open({ viewModel: EditConnection, model: new SavedConnection() })
            .then(result => {
                if(result.output) this.saveConnection(result.output)
            });
    }

    editConnection(connection) {
        this.dialogService.open({ viewModel: EditConnection, model: connection })
            .then(result => {
                if(result.output) this.saveConnection(result.output)
            });
    }

    deleteConnection(connection) {
        let index = this.savedConnections.indexOf(connection);
        this.savedConnections.splice(index, 1);
        localStorage.setItem('connections.saved', JSON.stringify(this.savedConnections));
    }

    saveConnection(connection: SavedConnection) {
        if(this.savedConnections.indexOf(connection) === -1) {
            this.savedConnections.push(connection);
        }
        localStorage.setItem('connections.saved', JSON.stringify(this.savedConnections));
    }
}
