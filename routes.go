package main

import (
	"github.com/ant0ine/go-json-rest/rest"
	"strconv"
	"time"
)

// API: Shows the partitions managed by this server
func showPartitions(w rest.ResponseWriter, r *rest.Request) {
	res := NewHypermediaResource()
	res.AddCurie("partitions", "/docs/rels/{rel}", true)

	res.Links["self"] = HypermediaLink{
		Href: "/partitions",
	}
	res.Links["partitions:add"] = HypermediaLink{
		Href: "/partitions/add",
	}
	res.Links["partitions:delete"] = HypermediaLink{
		Href:      "/partitions/delete/{server}/{partition}",
		Templated: true,
	}
	res.Links["partitions:read"] = HypermediaLink{
		Href:      "/partitions/read/{server}/{partition}",
		Templated: true,
	}

	type partitionInfo struct {
		Name      string    `json:"name"`
		Partition Partition `json:"partition"`
		Host      string    `json:"host"`
		Database  string    `json:"database"`
		Port      string    `json:"port"`
	}

	partitions := []partitionInfo{}
	for _, s := range cfg.Servers {
		for k, v := range s.Partitions {
			partitions = append(partitions, partitionInfo{
				Name:      k,
				Partition: v,
				Host:      s.Host,
				Database:  s.Database,
				Port:      s.Port,
			})
		}
	}
	res.Data["totalPartitions"] = len(partitions)
	res.Data["partitions"] = partitions

	res.Success()
	w.WriteJson(res.End("There are " + strconv.Itoa(len(partitions)) + " partitions managed."))
}

// API: Shows the maintenance schedule as currently configured
func showSchedule(w rest.ResponseWriter, r *rest.Request) {
	res := NewHypermediaResource()
	res.AddCurie("schedule", "/docs/rels/{rel}", true)

	res.Links["self"] = HypermediaLink{
		Href: "/schedule",
	}
	res.Links["schedule:add"] = HypermediaLink{
		Href: "/schedule/add",
	}
	res.Links["schedule:delete"] = HypermediaLink{
		Href:      "/schedule/delete/{id}",
		Templated: true,
	}

	jobs := []map[string]interface{}{}
	for _, item := range c.Entries() {
		m := make(map[string]interface{})
		m["id"] = item.Id
		m["name"] = item.Name
		m["next"] = item.Next
		m["prev"] = item.Prev
		m["job"] = getFunctionName(item.Job)
		jobs = append(jobs, m)
	}
	res.Data["totalJobs"] = len(jobs)
	res.Data["jobs"] = jobs

	res.Success()
	w.WriteJson(res.End("There are " + strconv.Itoa(len(jobs)) + " jobs scheduled."))
}

// API: Shows the partitions managed by this server
func showChildren(w rest.ResponseWriter, r *rest.Request) {
	res := NewHypermediaResource()

	res.Links["self"] = HypermediaLink{
		Href: "/partition/{server}/{partition}/children",
	}

	partitionName := r.PathParam("partition")
	serverName := r.PathParam("server")

	// queryParams := r.URL.Query()

	db, partition, err := GetPartition(serverName, partitionName)
	if err == nil {
		children := db.GetChildPartitions(partition)
		res.Data["totalChildren"] = len(children)
		res.Data["children"] = children
		res.Success()
		w.WriteJson(res.End("There are " + strconv.Itoa(len(children)) + " children for this partition."))
	} else {
		l.Error(err)
		w.WriteJson(res.End("The partition was not found."))
	}
}

// Inspired by a few hypermedia formats, this is a structure for Social Harvest API responses.
// Storing data into Social Harvest is easy...Getting it back out and having other widgets for the dashboard be able to talk with the API is the hard part.
// So a self documenting API that can be navigated automatically is super handy.
// NOTE: This is going to change a good bit at first.

// A resource is the root level item being returned. It can contain embedded resources if necessary. It's possible to return more than one resource at a time too (though won't be common).
// Within each resource there is "_meta" data
type HypermediaResource struct {
	Meta     HypermediaMeta                `json:"_meta"`
	Links    map[string]HypermediaLink     `json:"_links,omitempty"`
	Curies   map[string]HypermediaCurie    `json:"_curies,omitempty"`
	Data     map[string]interface{}        `json:"_data,omitempty"`
	Embedded map[string]HypermediaResource `json:"_embedded,omitempty"`
	Forms    map[string]HypermediaForm     `json:"_forms,omitempty"`
}

// The Meta structure provides some common information helpful to the application and also resource state.
type HypermediaMeta struct {
	startTime    time.Time
	Success      bool    `json:"success"`
	Message      string  `json:"message"`
	ResponseTime float32 `json:"responseTime,omitempty"`
	To           string  `json:"to,omitempty"`
	From         string  `json:"from,omitempty"`
}

// A simple web link structure (somewhat modeled after HAL's links and http://tools.ietf.org/html/rfc5988).
// NOTE: in HAL format, links can be an array with aliases - our format has no such support, but this doens't break HAL compatibility.
// Why not support it? Because that changes from {} to [] and changing data types is a burden for others. Plus we have HTTP 301/302.
// Also, each "_links" key name using this struct should be one of: http://www.iana.org/assignments/link-relations/link-relations.xhtml unless using CURIEs.
type HypermediaLink struct {
	Href        string `json:"href,omitempty"`
	Type        string `json:"type,omitempty"`
	Deprecation string `json:"deprecation,omitempty"`
	Name        string `json:"name,omitempty"`
	Profile     string `json:"profile,omitempty"`
	Title       string `json:"title,omitempty"`
	Hreflang    string `json:"hreflang,omitempty"`
	Templated   bool   `json:"templated,omitempty"`
}

// Defines a CURIE
type HypermediaCurie struct {
	Name      string `json:"name,omitempty"`
	Href      string `json:"href,omitempty"`
	Templated bool   `json:"templated,omitempty"`
}

// Form structure defines attributes that match HTML. This tells applications how to work with resources in order to manipulate state.
// Any attribute not found in HTML should be prefixed with an underscore (for example, "_fields").
type HypermediaForm struct {
	Name          string                         `json:"name,omitempty"`
	Method        string                         `json:"method,omitempty"`
	Enctype       string                         `json:"enctype"`
	AcceptCharset string                         `json:"accept-charset,omitempty"`
	Target        string                         `json:"target,omitempty"`
	Action        string                         `json:"action,omitempty"`
	Autocomplete  bool                           `json:"autocomplete,omitempty"`
	Fields        map[string]HypermediaFormField `json:"_fields,omitempty"`
}

// Defines properties for a field (HTML attributes) as well as holds the "_errors" and validation "_rules" for that field.
// "_rules" have key names that map to HypermediaFormField.Name, like { "fieldName": HypermediaFormFieldRule } and the rules themself are named.
// "_errors" have key names that also map to HypermediaFormField.Name
type HypermediaFormField struct {
	Name         string                              `json:"name,omitempty"`
	Value        string                              `json:"value,omitempty"`
	Type         string                              `json:"type,omitempty"`
	Src          string                              `json:"src,omitempty"`
	Checked      bool                                `json:"checked,omitempty"`
	Disabled     bool                                `json:"disabled,omitempty"`
	ReadOnly     bool                                `json:"readonly,omitempty"`
	Required     bool                                `json:"required,omitempty"`
	Autocomplete bool                                `json:"autocomplete,omitempty"`
	Tabindex     int                                 `json:"tabindex,omitempty"`
	Multiple     bool                                `json:"multiple,omitempty"`
	Accept       string                              `json:"accept,omitempty"`
	Errors       map[string]HypermediaFormFieldError `json:"_errors,omitempty"`
	Rules        map[string]HypermediaFormFieldRule  `json:"_rules,omitempty"`
}

// Error messages from validation failures (optional) "name" is the HypermediaFormFieldRule.Name in this case and "message" is returned on failure.
type HypermediaFormFieldError struct {
	Name    string `json:"name"`
	Failed  bool   `json:"name"`
	Message string `json:"message,omitempty"`
}

// Simple validation rules. Easily nested into "_rules" on "_fields" (optional). Of course front-end validation is merely convenience and not a trustable process.
// So remember to sanitize and validate any data on the server side of things. However, this does help tremendously in reducing the number of HTTP requests to the API.
type HypermediaFormFieldRule struct {
	Name        string                                         `json:"name"`
	Description string                                         `json:"description,omitempty"`
	Pattern     string                                         `json:"pattern"`
	Function    func(value string) (fail bool, message string) // not for JSON
}

// Conveniently sets a few things up for a resource
func NewHypermediaResource() *HypermediaResource {
	r := HypermediaResource{}
	r.Meta.startTime = time.Now()
	r.Links = make(map[string]HypermediaLink)
	r.Data = make(map[string]interface{})

	return &r
}

// Not necessary... But there may be some other functions that make sense...
func (h *HypermediaResource) Success() {
	h.Meta.Success = true
}

func (h *HypermediaResource) AddCurie(name string, href string, templated bool) {
	c := HypermediaCurie{}
	c.Name = name
	c.Href = href
	c.Templated = templated
	if len(h.Curies) < 1 {
		h.Curies = make(map[string]HypermediaCurie)
	}
	h.Curies[name] = c
}

// Conveniently sets a few things before returning the resource and optionally allows a passed string to set HypermediaResource.Meta.Message
func (h *HypermediaResource) End(message ...string) *HypermediaResource {
	if len(message) > 0 {
		h.Meta.Message = message[0]
	}
	h.Meta.ResponseTime = float32(time.Since(h.Meta.startTime).Seconds())
	return h
}
