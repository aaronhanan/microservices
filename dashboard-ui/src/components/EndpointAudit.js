import React, { useEffect, useState } from 'react'
import '../App.css';


export default function EndpointAudit(props) {
    const [isLoaded, setIsLoaded] = useState(false);
    const [log, setLog] = useState(null);
    const [error, setError] = useState(null)
    const [test, setTest] = useState([])
    const [index, setIndex] = useState(null);
	const rand_val = Math.floor((Math.random() * 100)); // Get a random event from the event store
    const getAudit = () => {
        fetch(`http://kafka-service.westus2.cloudapp.azure.com:8110/${props.endpoint}?index=${rand_val}`)
            .then(res => res.json())
            .then((result)=>{
				console.log("Received Audit Results for " + props.endpoint)
                result['index_val'] = rand_val;
                if (result.message == "Not Found") {
                    result = {index_val: rand_val, customer_id: "Not Found", name: "Not Found", phone: "Not Found", order_date: "Not Found"}
                }
                setLog(result);
                setTest([...test, result])
                setIndex(rand_val)
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
	useEffect(() => {
		const interval = setInterval(() => getAudit(), 4000); // Update every 4 seconds
		return() => clearInterval(interval);
    }, [getAudit]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        // test.map((order) =>
    const all_requests = (
        <div style={{width:"800px", marginLeft:"auto", marginRight:"auto", marginBottom:"80px"}}>
            <table id="customers">
                <tr>
                    <th>Index</th>
                    <th>Customer ID</th>
                    <th>Name</th>
                    <th>Phone</th>
                    <th>Date</th>
                </tr>
            {test.map((order) =>
            <tr>
                <td>{order.index_val}</td>
                <td>{order.customer_id}</td>
                <td>{order.name}</td>
                <td>{order.phone}</td>
                <td>{order.order_date}</td>
            </tr>   
            )}
            </table>
        </div>
    )

    
        
        return (
            <div>
                <div>
                    <h3>{props.endpoint}: {index}</h3>
                    {/* {JSON.stringify(log)} */}
                </div>
                <div style={{width:"800px", marginLeft:"auto", marginRight:"auto", marginBottom:"80px"}}>
                    <table id="customers">
                        <tr>
                            <th>ID</th>
                            <th>Name</th>
                            <th>Phone</th>
                            <th>Date</th>
                        </tr>
                        <tr>
                            <td>{log.customer_id}</td>
                            <td>{log.name}</td>
                            <td>{log.phone}</td>
                            <td>{log.order_date}</td>
                        </tr>
                    </table>
                </div>
                {all_requests}
                <hr />
            </div>
        )
    }
}
