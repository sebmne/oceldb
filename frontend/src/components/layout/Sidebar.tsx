import { NavLink } from "react-router-dom";

const links = [
  { to: "/overview", label: "Overview" },
  { to: "/browse", label: "Browse Tables" },
  { to: "/process-map", label: "Process Map" },
  { to: "/sql", label: "SQL Console" },
];

export default function Sidebar() {
  return (
    <aside className="flex w-56 flex-col border-r border-slate-200/80 bg-slate-950">
      <div className="px-5 py-5">
        <span className="text-lg font-bold tracking-tight text-white">
          oceldb
        </span>
        <span className="ml-1.5 rounded bg-blue-600 px-1.5 py-0.5 text-[10px] font-semibold text-white">
          UI
        </span>
      </div>

      <nav className="flex-1 space-y-0.5 px-3 pt-2">
        {links.map(({ to, label }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `block rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
                isActive
                  ? "bg-blue-600 text-white"
                  : "text-slate-400 hover:bg-slate-800 hover:text-white"
              }`
            }
          >
            {label}
          </NavLink>
        ))}
      </nav>

      <div className="px-5 py-4 text-[10px] text-slate-600">
        oceldb v0.2.0
      </div>
    </aside>
  );
}
