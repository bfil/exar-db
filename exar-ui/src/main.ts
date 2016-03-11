import 'bootstrap';
import 'bootstrap-datetimepicker';
import 'moment';

import {Aurelia} from 'aurelia-framework';

export function configure(aurelia: Aurelia) {
    aurelia.use
        .standardConfiguration()
        .developmentLogging()
        .globalResources([
			'converters/date-format',
            'converters/upper'
		]);

    aurelia.use.plugin('aurelia-animator-css');

    // Anyone wanting to use HTMLImports to load views, will need to install the following plugin.
    // aurelia.use.plugin('aurelia-html-import-template-loader')

    aurelia.start().then(() => aurelia.setRoot());
}
