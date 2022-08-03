geneid_color = "#d5abff";
commonname_color = "#abffff";
compound_color = "#d5ffab";
species_color = "#ffffab";
location_color = "#ffd5ab";
process_color = "#ffabab";

nodes = new vis.DataSet([
    {"id": -1, "label": "GeneID", "color": geneid_color},
    {"id": -2, "label": "CommonName", "color": commonname_color},
    {"id": -3, "label": "CommonName", "color": commonname_color},
    {"id": -4, "label": "Compound", "color": compound_color},
    {"id": -5, "label": "Species", "color": species_color},
    {"id": -6, "label": "Location", "color": location_color},
    {"id": -7, "label": "Process", "color": process_color},
]);

edges = new vis.DataSet([
    {"from": -1, "to": -2},
    {"from": -2, "to": -3},
    {"from": -2, "to": -4},
    {"from": -2, "to": -5},
    {"from": -2, "to": -6},
    {"from": -2, "to": -7},

]);

container = document.getElementById("div_graph");
data = {
    nodes: nodes,
    edges: edges,
};
options = {};
network = new vis.Network(container, data, options);


function run_load_gene_list(){
    document.getElementById("div_status").innerHTML = "Loading...";

    sl_gene = document.getElementById("sl_gene");
    sl_gene.innerHTML = "";
    option = document.createElement("option");
    option.value = "--select--";
    option.text = "--select--";
    sl_gene.appendChild(option);

    request_data = {
        "plant": document.getElementById("sl_plant").value,
    };

    fetch("./run_load_gene_list", {method: "post", body: JSON.stringify(request_data)})
    .then(function(response) {
        return response.json();
    })
    .then(function(response_data) {
        geneid_list = response_data["geneid_list"];

        for (geneid of geneid_list)
        {
            option = document.createElement("option");
            option.value = geneid;
            option.text = geneid;
            sl_gene.appendChild(option);
        }

        document.getElementById("div_status").innerHTML = "Ready";
    })
}


function run_select_gene_id(){
    document.getElementById("div_status").innerHTML = "Loading...";
    document.getElementById("ta_gene").value = document.getElementById("sl_gene").value;
    document.getElementById("div_status").innerHTML = "Ready";
}


function run_generate_graph(){
    document.getElementById("div_status").innerHTML = "Loading...";

    request_data = {
        "geneid": document.getElementById("ta_gene").value,
    };

    fetch("./run_generate_graph", {method: "post", body: JSON.stringify(request_data)})
    .then(function(response) {
        return response.json();
    })
    .then(function(response_data) {
        node_list = response_data["node_list"];
        edge_list = response_data["edge_list"];

        network.destroy();

        container = document.getElementById("div_graph");
        data = {
            nodes: new vis.DataSet(node_list),
            edges: new vis.DataSet(edge_list),
        };
        options = {};
        network = new vis.Network(container, data, options);

        document.getElementById("div_status").innerHTML = "Ready";
    })
}
