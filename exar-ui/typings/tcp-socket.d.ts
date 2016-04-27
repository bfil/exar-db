interface TCPSocketStatic {
    open(host: string, port: number): TCPSocket;
}

interface TCPSocket {
    send(data: ArrayBufferView);
    close();
    
    onopen: any;
    ondata: (ArrayBufferView) => any;
    onerror: any;
    onclose: any;
    ondrain: any;
}

interface Navigator {
    TCPSocket: TCPSocketStatic;
}