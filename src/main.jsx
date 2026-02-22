import React from 'react'
import ReactDOM from 'react-dom/client'
import App from '@/App.jsx'
import '@/index.css'
import { ThemeProvider } from "@/components/theme-provider.jsx"

ReactDOM.createRoot(document.getElementById('root')).render(
    <ThemeProvider attribute="class" defaultTheme="light" storageKey="alphastock-theme">
        <App />
    </ThemeProvider>
) 
