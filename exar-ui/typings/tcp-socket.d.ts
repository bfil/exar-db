interface TCPSocket {
    open(host: string, port: number): TCPSocket;
    send(data: any);
    
    onopen: any;
    ondata: any;
    onerror: any;
    onclose: any;
    ondrain: any;
}

interface Navigator {
    TCPSocket: TCPSocket;
}