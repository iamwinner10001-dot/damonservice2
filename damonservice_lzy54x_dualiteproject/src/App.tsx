import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import Login from './pages/Login';
import Dashboard from './pages/Dashboard';
import ProjectList from './pages/ProjectList';
import ProjectDetails from './pages/ProjectDetails';
import MapOverview from './pages/MapOverview';

// Admin Pages
import AdminUsers from './pages/admin/Users';
import AdminCategories from './pages/admin/Categories';
import AdminDevices from './pages/admin/Devices';
import AdminSettings from './pages/admin/Settings';
import AdminAuditLogs from './pages/admin/AuditLogs';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<Login />} />
        
        <Route path="/" element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="projects" element={<ProjectList />} />
          <Route path="projects/:id" element={<ProjectDetails />} />
          <Route path="map" element={<MapOverview />} />
          
          {/* Admin Management Routes */}
          <Route path="admin/users" element={<AdminUsers />} />
          <Route path="admin/categories" element={<AdminCategories />} />
          <Route path="admin/devices" element={<AdminDevices />} />
          <Route path="admin/settings" element={<AdminSettings />} />
          <Route path="admin/audit" element={<AdminAuditLogs />} />
        </Route>

        <Route path="*" element={<Navigate to="/" />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
