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
        port: 5173,
        proxy: {
            "/getPib": "http://127.0.0.1:5003",
            "/pibChat": "http://127.0.0.1:5003",
            "/deleteIndexSession": "http://127.0.0.1:5003",
            "/getAllDocumentRuns": "http://127.0.0.1:5003",
            "/getAllIndexSessions": "http://127.0.0.1:5003",
            "/getAllSessions": "http://127.0.0.1:5003",
            "/getCashFlow": "http://127.0.0.1:5003",
            "/getIncomeStatement": "http://127.0.0.1:5003",
            "/getIndexSession": "http://127.0.0.1:5003",
            "/getIndexSessionDetail": "http://127.0.0.1:5003",
            "/getSocialSentiment": "http://127.0.0.1:5003",
            "/getNews": "http://127.0.0.1:5003",
            "/renameIndexSession": "http://127.0.0.1:5003",
            "/getSuggestedQuestions": "http://127.0.0.1:5003",
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
