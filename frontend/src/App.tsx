import { Routes, Route, Navigate } from "react-router-dom";
import Layout from "./components/layout/Layout";
import OverviewPage from "./pages/OverviewPage";
import BrowserPage from "./pages/BrowserPage";
import ProcessMapPage from "./pages/ProcessMapPage";
import SqlConsolePage from "./pages/SqlConsolePage";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/overview" replace />} />
        <Route path="overview" element={<OverviewPage />} />
        <Route path="browse" element={<BrowserPage />} />
        <Route path="process-map" element={<ProcessMapPage />} />
        <Route path="sql" element={<SqlConsolePage />} />
      </Route>
    </Routes>
  );
}
