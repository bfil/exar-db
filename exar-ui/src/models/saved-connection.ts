export class SavedConnection {
    alias: string;
    requiresAuth: boolean;
    host: string = 'localhost';
    port: number = 38580;
    username: string;
    password: string;
}
