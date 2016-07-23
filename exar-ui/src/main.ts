import 'bootstrap';
import 'emailjs-tcp-socket';

import {Aurelia} from 'aurelia-framework';

export function configure(aurelia: Aurelia) {
    aurelia.use
        .standardConfiguration()
        .developmentLogging()
        .plugin('aurelia-animator-css')
        .plugin('aurelia-dialog', config => {
            config.useDefaults();
            config.settings.lock = false;
        })
        .globalResources([
            'converters/obfuscate'
		]);

    aurelia.start().then(() => aurelia.setRoot());
}
