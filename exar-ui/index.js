//Note: This file is provided as an aid to help you get up and running with
//Electron for desktop apps. See the readme file for more information.

'use strict';

const electron = require('electron');
const { app, BrowserWindow } = electron;

// require('crash-reporter').start();

var mainWindow = null;

app.on('window-all-closed', function () {
    if (process.platform != 'darwin') {
        app.quit();
    }
});

app.on('ready', function () {
    mainWindow = new BrowserWindow({
        width: 800,
        height: 600
    });
    
    // mainWindow.webContents.openDevTools();

    mainWindow.loadURL('file://' + __dirname + '/index.html');
    mainWindow.webContents.on('did-finish-load', function () {
        mainWindow.setTitle(app.getName());
    });
    mainWindow.on('closed', function () {
        mainWindow = null;
    });
});
