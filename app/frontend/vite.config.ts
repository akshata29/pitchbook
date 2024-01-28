import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import Markdown from '@pity/vite-plugin-react-markdown'
import EnvironmentPlugin from 'vite-plugin-environment'

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [react(), 
        EnvironmentPlugin('all'),
        Markdown({
        wrapperComponentName: 'ReactMarkdown',
       // wrapperComponentPath: './src/pages/help/help',
      })],
    build: {
        outDir: "../backend/static",
        emptyOutDir: true,
        sourcemap: true
    },
    server: {
        proxy: {
            "/getPib": "http://127.0.0.1:5001",
            "/pibChat": "http://127.0.0.1:5001",
            "/deleteIndexSession": "http://127.0.0.1:5001",
            "/getAllDocumentRuns": "http://127.0.0.1:5001",
            "/getAllIndexSessions": "http://127.0.0.1:5001",
            "/getAllSessions": "http://127.0.0.1:5001",
            "/getCashFlow": "http://127.0.0.1:5001",
            "/getIncomeStatement": "http://127.0.0.1:5001",
            "/getIndexSession": "http://127.0.0.1:5001",
            "/getIndexSessionDetail": "http://127.0.0.1:5001",
            "/getSocialSentiment": "http://127.0.0.1:5001",
            "/getNews": "http://127.0.0.1:5001",
            "/renameIndexSession": "http://127.0.0.1:5001",
        }
        // proxy: {
        //     "/ask": {
        //          target: 'http://127.0.0.1:5000',
        //          changeOrigin: true,
        //          secure: false,
        //      }
        // }
    }
});
