export class ObfuscateValueConverter {
    toView(value) {
        return Array(value.length).join('*');
    }
}
