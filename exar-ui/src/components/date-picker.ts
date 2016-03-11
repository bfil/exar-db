import {autoinject, bindingMode, bindable} from 'aurelia-framework';
import * as $ from 'jquery';
import * as moment from 'moment';

@autoinject
export class DatePicker {

    @bindable format = "YYYY-MM-DD";
    @bindable({
        defaultBindingMode: bindingMode.twoWay,
    }) value;
    
    element: Element;
    datePicker: any;

    constructor(element: Element) {
        this.element = element;
     }

    attached() {        
        this.datePicker = $(this.element).find('.input-group.date')
            .datetimepicker({
                format: this.format,
                showClose: true,
                showTodayButton: true,
                ignoreReadonly: true
            });

        this.datePicker.on("dp.change", (e) => {
            this.value = moment(e.date).format(this.format);
        });
    }
}