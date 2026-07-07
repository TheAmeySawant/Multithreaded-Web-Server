use std::{
    fs,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};
use multithreaded_web_server::ThreadPool;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").expect("Network Binding Failed!");

    //As long as size is positive, ThreadPool will be created.
    let pool = ThreadPool::new(8).unwrap();
    for stream in listener.incoming() {
        let stream = stream.expect("TCPStream lost, maybe connection lost");

        pool.execute(|| {
            handle_connection(stream);
        });

        // thread::spawn(|| {
        //     handle_connection(stream);
        // });

        // handle_connection(stream);

        // println!("Connection Established!");
    }
}

fn handle_connection(mut stream: TcpStream) {
    let buf_reader = BufReader::new(&stream);

    let request_line = buf_reader
        .lines()
        .next()
        .expect("buf_reader is empty. Found None")
        .expect("buf_reader to request_line conversion failed!");

    let (status_line, filename) = match &request_line[..] {
        "GET / HTTP/1.1" => ("HTTP/1.1 200 OK", "pages/home.html"),
        "GET /sleep HTTP/1.1" => {
            thread::sleep(Duration::from_secs(5));

            ("HTTP/1.1 200 OK", "pages/sleep.html")
        }
        _ => ("HTTP/1.1 404 OK", "pages/404.html"),
    };

    let content = fs::read_to_string(filename).expect("file reading failed");

    let length = content.len();

    let response = format!("{status_line}\r\nContent-Length: {length} \r\n\r\n {content}");

    stream
        .write_all(response.as_bytes())
        .expect("Sending response failed");

    println!("Request: {request_line:#?}");
}
