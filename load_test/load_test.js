import http from 'k6/http';

export const options = {
    vus: 4,
    iterations: 4,
};

export default function () {
    http.get('http://127.0.0.1:7878/sleep');
}