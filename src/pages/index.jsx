import Layout from "./Layout.jsx";

import ControlCenter from "./ControlCenter";
import Screener from "./Screener";

import Lenses from "./Lenses";

import Projection from "./Projection";
import Portfolio from "./Portfolio";
import Portfolios from "./Portfolios";
import Login from "./Login";
import AdminAccounts from "./AdminAccounts";
import Notifications from "./Notifications";
import Snapshots from "./Snapshots";
import WatchlistPage from "./Watchlist";

import { BrowserRouter as Router, Route, Routes, useLocation } from 'react-router-dom';

const PAGES = {

    ControlCenter: ControlCenter,
    Screener: Screener,
    
    Lenses: Lenses,
    
    Projection: Projection,
    Portfolio: Portfolio,
    Portfolios: Portfolios,
    Accounts: AdminAccounts,
    Notifications: Notifications,
    Snapshots: Snapshots,
    Watchlist: WatchlistPage,

}

function _getCurrentPage(url) {
    if (url.endsWith('/')) {
        url = url.slice(0, -1);
    }
    let urlLastPart = url.split('/').pop();
    if (urlLastPart.includes('?')) {
        urlLastPart = urlLastPart.split('?')[0];
    }

    const pageName = Object.keys(PAGES).find(page => page.toLowerCase() === urlLastPart.toLowerCase());
    return pageName || Object.keys(PAGES)[0];
}

// Create a wrapper component that uses useLocation inside the Router context
function PagesContent() {
    const location = useLocation();
    const currentPage = _getCurrentPage(location.pathname);
    
    return (
        <Layout currentPageName={currentPage}>
            <Routes>            
                
                    <Route path="/" element={<ControlCenter />} />
                <Route path="/controlcenter" element={<ControlCenter />} />
                
                
                <Route path="/Screener" element={<Screener />} />
                
                <Route path="/Lenses" element={<Lenses />} />
                
                <Route path="/Projection" element={<Projection />} />
                <Route path="/Portfolio" element={<Portfolio />} />
                <Route path="/Portfolios" element={<Portfolios />} />
                <Route path="/portfolios" element={<Portfolios />} />
                <Route path="/Accounts" element={user.role === "admin" ? <AdminAccounts /> : <Navigate replace to="/" />} />
                <Route path="/notifications" element={<Notifications />} />
                <Route path="/Notifications" element={<Notifications />} />
                <Route path="/snapshots" element={<Snapshots />} />
                <Route path="/Snapshots" element={<Snapshots />} />
                <Route path="/watchlist" element={<WatchlistPage />} />
                <Route path="/Watchlist" element={<WatchlistPage />} />
                <Route path="/login" element={<Navigate replace to="/" />} />
                
            </Routes>
        </Layout>
    );
}

export default function Pages() {
    return (
        <Router>
            <PagesContent />
        </Router>
    );
}
