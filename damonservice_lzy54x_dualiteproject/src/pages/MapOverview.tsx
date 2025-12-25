import { useEffect, useState } from 'react';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import { api, Project } from '../services/api';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';

// Fix Leaflet Icon
import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';

let DefaultIcon = L.icon({
    iconUrl: icon,
    shadowUrl: iconShadow,
    iconSize: [25, 41],
    iconAnchor: [12, 41]
});
L.Marker.prototype.options.icon = DefaultIcon;

export default function MapOverview() {
  const [projects, setProjects] = useState<Project[]>([]);

  useEffect(() => {
    api.getProjects().then(res => {
      if (res.ok) setProjects(res.data);
    });
  }, []);

  const validProjects = projects.filter(p => p.tehran_lat && p.tehran_lng);

  return (
    <div className="h-[calc(100vh-140px)] rounded-xl overflow-hidden border border-gray-200 shadow-sm relative z-0">
      <MapContainer center={[35.6892, 51.3890]} zoom={12} scrollWheelZoom={true} className="h-full w-full">
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {validProjects.map(p => (
          <Marker key={p.id} position={[p.tehran_lat!, p.tehran_lng!]}>
            <Popup>
              <div className="text-right font-vazir" dir="rtl">
                <h3 className="font-bold text-sm mb-1">{p.project_name}</h3>
                <p className="text-xs text-gray-600">{p.employer_name}</p>
                <div className={`mt-2 text-xs px-2 py-1 rounded inline-block ${
                  p.status === 'approved' ? 'bg-green-100 text-green-700' : 
                  p.status === 'rejected' ? 'bg-red-100 text-red-700' : 'bg-yellow-100 text-yellow-700'
                }`}>
                  {p.status}
                </div>
              </div>
            </Popup>
          </Marker>
        ))}
      </MapContainer>
      
      <div className="absolute top-4 right-4 z-[1000] bg-white p-3 rounded-lg shadow-md max-w-xs">
        <h3 className="font-bold text-sm mb-2">راهنما</h3>
        <div className="text-xs space-y-1 text-gray-600">
          <p>تعداد کل پروژه‌های دارای موقعیت: {validProjects.length}</p>
          <p>برای مشاهده جزئیات روی پین‌ها کلیک کنید.</p>
        </div>
      </div>
    </div>
  );
}
