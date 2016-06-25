import {Router, RouterConfiguration} from 'aurelia-router';

export class App {
    router: Router;

    configureRouter(config: RouterConfiguration, router: Router) {
        config.title = 'Exar UI';
        config.map([
            { route: '', name: 'home', moduleId: 'views/home', nav: false, title: 'Home' },
            { route: 'settings', name: 'settings', moduleId: 'views/settings', nav: false, title: 'Settings' }
        ]);
        this.router = router;
    }
}
