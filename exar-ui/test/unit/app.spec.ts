import { Container } from 'aurelia-framework';
import { Router, RouterConfiguration } from 'aurelia-router';

import { App } from 'src/app';

describe('App', () => {
    var app: App,
        routerConfig: RouterConfiguration,
        router: Router;

    beforeEach(() => {
        let container = new Container();

        router = container.get(Router);
        routerConfig = container.get(RouterConfiguration);

        spyOn(routerConfig, 'map');

        app = new App();
    });

    it('should configure the router correctly', () => {
        app.configureRouter(routerConfig, router);

        expect(app.router).toBeDefined();
        expect(routerConfig.title).toEqual('Exar UI');
        expect(routerConfig.map).toHaveBeenCalled();
    });
});
