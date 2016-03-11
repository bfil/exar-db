import {Router, RouterConfiguration} from 'aurelia-router';

export class App {
    router: Router;

    configureRouter(config: RouterConfiguration, router: Router) {
        config.title = 'Exar UI';
        config.map([
            { route: '', name: 'home', moduleId: 'views/home', nav: false, title: 'Home' }
        ]);
        this.router = router;
    }
}
