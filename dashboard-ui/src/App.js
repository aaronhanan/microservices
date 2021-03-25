import logo from './logo.jpg';
import './App.css';

import EndpointAudit from './components/EndpointAudit'
import AppStats from './components/AppStats'

function App() {

    const endpoints = ["food_order", "scheduled_order"]

    const rendered_endpoints = endpoints.map((endpoint) => {
        return <EndpointAudit key={endpoint} endpoint={endpoint}/>
    })

    return (
        <div className="App">
            <img src={logo} className="App-logo" alt="logo" height="350px" width="400px" style={{marginBottom:"-80px", marginTop:"-70px"}}/>
            <div>
                <AppStats/>
                <h1>Audit Endpoints</h1>
                {rendered_endpoints}
            </div>
        </div>
    );

}



export default App;
