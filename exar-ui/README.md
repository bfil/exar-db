# Exar UI

A basic Exar DB's user interface, built with [Aurelia](http://aurelia.io), [Electron](http://electron.atom.io) and [TypeScript](https://www.typescriptlang.org)

## Running The App

To run the app, follow these steps.

1. Ensure that [NodeJS](http://nodejs.org/) is installed. This provides the platform on which the build tooling runs.
2. From the project folder, execute the following command:
  ```shell
  npm install
  ```
3. Ensure that [Gulp](http://gulpjs.com/) is installed globally. If you need to install it, use the following command:
  ```shell
  npm install -g gulp
  ```
  > **Note:** Gulp must be installed globally, but a local version will also be installed to ensure a compatible version is used for the project.
4. Ensure that [jspm](http://jspm.io/) is installed globally. If you need to install it, use the following command:
  ```shell
  npm install -g jspm
  ```
  > **Note:** jspm must be installed globally, but a local version will also be installed to ensure a compatible version is used for the project.

  > **Note:** Sometimes jspm queries GitHub to install packages, but GitHub has a rate limit on anonymous API requests. If you receive a rate limit error, you need to configure jspm with your GitHub credentials. You can do this by executing `jspm registry config github` and following the prompts. If you choose to authorize jspm by an access token instead of giving your password (see GitHub `Settings > Personal Access Tokens`), `public_repo` access for the token is required.
5. Install the client-side dependencies with jspm:

  ```shell
  jspm install -y
  ```
  >**Note:** Windows users, if you experience an error of "unknown command unzip" you can solve this problem by doing `npm install -g unzip` and then re-running `jspm install`.

6. Build the project:

  ```shell
  gulp build
  ```

7. Install [Electron](http://electron.atom.io)

  ```shell
  npm install electron-prebuilt -g
  ```
8. To start the app, execute the following command:

  ```shell
  electron index.js
  ```
>**Note:** If you use electron every time or are packaging and so-forth, Then change this line in package.json from
`"main": "dist/main.js",` to `"main": "index.js",`
Build the app (this will give you a dist directory)
```shell
gulp build
```
To start the app, execute the following command:
```shell
   electron .
```

## Development

1. Run the app in watch mode, execute the following command:

  ```shell
  gulp watch
  ```

2. Then start the app in electron in a separate terminal, execute the following command:

  ```shell
  electron index.js
  ```

## Running The Unit Tests

To run the unit tests, first ensure that you have followed the steps above in order to install all dependencies and successfully build the library. Once you have done that, proceed with these additional steps:

1. Ensure that the [Karma](http://karma-runner.github.io/) CLI is installed. If you need to install it, use the following command:

  ```shell
  npm install -g karma-cli
  ```
2. Install Aurelia libs for test visibility:

```shell
jspm install aurelia-framework
jspm install aurelia-http-client
jspm install aurelia-router
```
3. You can now run the tests with this command:

  ```shell
  karma start
  ```
