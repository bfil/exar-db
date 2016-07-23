import {autoinject, bindable} from 'aurelia-framework';
import {Router} from 'aurelia-router';

@autoinject
export class NavBar {

    @bindable router: Router;

    element: Element;

    constructor(element: Element) {
        this.element = element;
    }
}