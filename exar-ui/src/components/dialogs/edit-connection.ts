import {autoinject} from 'aurelia-framework';
import {DialogController} from 'aurelia-dialog';

import {SavedConnection} from 'models/saved-connection';

@autoinject
export class EditConnection {
    connection: SavedConnection;
    dialogController: DialogController;

    constructor(dialogController: DialogController) {
        this.dialogController = dialogController;
    }

    activate(connection: SavedConnection) {
        this.connection = connection;
    }

    saveConnection() {
        this.dialogController.ok(this.connection);
    }

    cancelConnection() {
        this.dialogController.cancel()
    }
}
