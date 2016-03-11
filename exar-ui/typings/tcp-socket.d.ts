interface TCPSocketStatic {
    open(host: string, port: number): TCPSocket;
}

interface TCPSocket {
    send(data: any);
    close();
    
    onopen: any;
    ondata: any;
    onerror: any;
    onclose: any;
    ondrain: any;
}

interface Navigator {
    TCPSocket: TCPSocketStatic;
}