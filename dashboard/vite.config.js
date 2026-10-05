import { defineConfig } from 'vite';
export default defineConfig({server:{proxy:{'/api/orders':'http://localhost:8081','/api/stock':'http://localhost:8082','/api/metrics':'http://localhost:8083'}}});
