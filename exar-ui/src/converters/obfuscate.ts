export class ObfuscateValueConverter {
    toView(value) {
        if(value) return Array(value.length).join('*');
        else return '';
    }
}
