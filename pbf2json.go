package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"

	geo "github.com/paulmach/go.geo"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type settings struct {
	PbfPath    string
	LevedbPath string
	Tags       map[string][]string
	BatchSize  int
	WayNodes   bool
}

var emptyLatLons = make([]map[string]string, 0)
var offsetStartWays int64

func getSettings() settings {

	// command line flags
	leveldbPath := flag.String("leveldb", "/tmp", "path to leveldb directory")
	tagList := flag.String("tags", "", "comma-separated list of valid tags, group AND conditions with a +")
	batchSize := flag.Int("batch", 50000, "batch leveldb writes in batches of this size")
	wayNodes := flag.Bool("waynodes", false, "should the lat/lons of nodes belonging to ways be printed")

	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		log.Fatal("invalid args, you must specify a PBF file")
	}

	// invalid tags
	if len(*tagList) < 1 {
		log.Fatal("Nothing to do, you must specify tags to match against")
	}

	// parse tag conditions
	conditions := make(map[string][]string)
	for _, group := range strings.Split(*tagList, ",") {
		conditions[group] = strings.Split(group, "+")
	}

	// fmt.Print(conditions, len(conditions))
	// os.Exit(1)

	return settings{args[0], *leveldbPath, conditions, *batchSize, *wayNodes}
}

func main() {

	// configuration
	config := getSettings()

	// open pbf file
	file := openFile(config.PbfPath)
	defer file.Close()

	// perform two passes over the file, on the first pass
	// we record a bitmask of the interesting elements in the
	// file, on the second pass we extract the data

	// set up bimasks
	var masks = NewBitmaskMap()

	// set up leveldb connection
	var db = openLevelDB(config.LevedbPath)
	defer db.Close()

	// === first pass (indexing) ===
	idxDecoder := osmpbf.New(context.Background(), file, runtime.GOMAXPROCS(-1))

	// index target IDs in bitmasks
	index(idxDecoder, masks, config)
	idxDecoder.Close()

	// no-op if no relation members of type 'way' present in mask
	if !masks.RelWays.Empty() {
		// === potential second pass (indexing) to index members of relations ===
		file.Seek(offsetStartWays, io.SeekStart) // rewind file
		idxRelationsDecoder := osmpbf.New(context.Background(), file, runtime.GOMAXPROCS(-1))

		// index relation member IDs in bitmasks
		indexRelationMembers(idxRelationsDecoder, masks, config)
		idxRelationsDecoder.Close()
	}

	// === final pass (printing json) ===
	file.Seek(0, io.SeekStart) // rewind file
	decoder := osmpbf.New(context.Background(), file, runtime.GOMAXPROCS(-1))

	// print json
	print(decoder, masks, db, config)
	decoder.Close()
}

func index(s *osmpbf.Scanner, masks *BitmaskMap, config settings) {
	for s.Scan() {
		switch v := s.Object().(type) {
		case *osm.Node:
			if hasTags(v.Tags) && containsValidTags(v.Tags, config.Tags) {
				masks.Nodes.Insert(int64(v.ID))
			}

		case *osm.Way:
			if offsetStartWays == 0 {
				offsetStartWays = s.PreviousFullyScannedBytes()
			}

			if hasTags(v.Tags) && containsValidTags(v.Tags, config.Tags) {
				masks.Ways.Insert(int64(v.ID))
				for _, node := range v.Nodes {
					masks.WayRefs.Insert(int64(node.ID))
				}
			}

		case *osm.Relation:
			if hasTags(v.Tags) && containsValidTags(v.Tags, config.Tags) {

				// record a count of which type of members
				// are present in the relation
				var hasWays bool
				for _, member := range v.Members {
					if member.Type == "way" {
						hasWays = true
						break
					}
				}

				// skip relations which contain 0 ways
				if !hasWays {
					continue
				}

				masks.Relations.Insert(int64(v.ID))
				for _, member := range v.Members {
					switch member.Type {
					case "node": // node
						masks.RelNodes.Insert(member.Ref)
					case "way": // way
						masks.RelWays.Insert(member.Ref)
					case "relation": // relation
						masks.RelRelation.Insert(member.Ref)
					}
				}
			}
		}
	}

	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
}

func indexRelationMembers(s *osmpbf.Scanner, masks *BitmaskMap, config settings) {
	for s.Scan() {
		switch v := s.Object().(type) {
		case *osm.Way:
			if masks.RelWays.Has(int64(v.ID)) {
				for _, node := range v.Nodes {
					masks.RelNodes.Insert(int64(node.ID))
				}
			}
		case *osm.Relation:
			break
			// support for super-relations
			// if masks.RelRelation.Has(int64(v.ID)) {
			// 	for _, member := range v.Members {
			// 		switch member.Type {
			// 		case "node": // node
			// 			masks.RelNodes.Insert(member.Ref)
			// 		case "way": // way
			// 			masks.RelWays.Insert(member.Ref)
			// 		}
			// 	}
			// }
		}
	}

	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
}

func print(s *osmpbf.Scanner, masks *BitmaskMap, db *leveldb.DB, config settings) {

	batch := new(leveldb.Batch)
	finishedNodes := false
	finishedWays := false

	for s.Scan() {
		switch v := s.Object().(type) {
		case *osm.Node:

			// ----------------
			// write to leveldb
			// note: only write way refs and relation member nodes
			// ----------------
			if masks.WayRefs.Has(int64(v.ID)) || masks.RelNodes.Has(int64(v.ID)) {

				// write in batches
				cacheQueueNode(batch, v)
				if batch.Len() > config.BatchSize {
					cacheFlush(db, batch, true)
				}
			}

			// bitmask indicates if this is a node of interest
			// if so, print it
			if masks.Nodes.Has(int64(v.ID)) {

				// trim tags
				trimTags(v.Tags)
				onNode(v)
			}

		case *osm.Way:

			// ----------------
			// write to leveldb
			// flush outstanding node batches
			// before processing any ways
			// ----------------
			if !finishedNodes {
				finishedNodes = true
				if batch.Len() > 1 {
					cacheFlush(db, batch, true)
				}
			}

			// ----------------
			// write to leveldb
			// note: only write relation member ways
			// ----------------
			if masks.RelWays.Has(int64(v.ID)) {

				// write in batches
				cacheQueueWay(batch, v)
				if batch.Len() > config.BatchSize {
					cacheFlush(db, batch, true)
				}
			}

			// bitmask indicates if this is a way of interest
			// if so, print it
			if masks.Ways.Has(int64(v.ID)) {

				// lookup from leveldb
				latlons, err := cacheLookupNodes(db, v)

				// skip ways which fail to denormalize
				if err != nil {
					break
				}

				// compute centroid
				centroid, bounds := computeCentroidAndBounds(latlons)

				// trim tags
				trimTags(v.Tags)

				if config.WayNodes {
					onWay(v, latlons, centroid, bounds)
				} else {
					onWay(v, emptyLatLons, centroid, bounds)
				}
			}

		case *osm.Relation:

			// ----------------
			// write to leveldb
			// flush outstanding way batches
			// before processing any relation
			// ----------------
			if !finishedWays {
				finishedWays = true
				if batch.Len() > 1 {
					cacheFlush(db, batch, true)
				}
			}

			// bitmask indicates if this is a relation of interest
			// if so, print it
			if masks.Relations.Has(int64(v.ID)) {

				// fetch all latlons for all ways in relation
				var memberWayLatLons = findMemberWayLatLons(db, v)

				// no ways found, skip relation
				if len(memberWayLatLons) == 0 {
					log.Println("[warn] denormalize failed for relation:", v.ID, "no ways found")
					continue
				}

				// best centroid and bounds to use
				var largestArea = 0.0
				var centroid map[string]string
				var bounds *geo.Bound

				// iterate over each way, selecting the largest way to use
				// for the centroid and bbox
				for _, latlons := range memberWayLatLons {

					// compute centroid
					wayCentroid, wayBounds := computeCentroidAndBounds(latlons)

					// if for any reason we failed to find a valid bounds
					if nil == wayBounds {
						log.Println("[warn] failed to calculate bounds for relation member way")
						continue
					}

					area := math.Max(wayBounds.GeoWidth(), 0.000001) * math.Max(wayBounds.GeoHeight(), 0.000001)

					// find the way with the largest area
					if area > largestArea {
						largestArea = area
						centroid = wayCentroid
						bounds = wayBounds
					}
				}

				// if for any reason we failed to find a valid bounds
				if nil == bounds {
					log.Println("[warn] denormalize failed for relation:", v.ID, "no valid bounds")
					continue
				}

				// trim tags
				trimTags(v.Tags)

				// print relation
				onRelation(v, centroid, bounds)
			}

		default:

			log.Fatalf("[error] unknown type %T\n", v)

		}
	}

	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
}

// lookup all latlons for all ways in relation
func findMemberWayLatLons(db *leveldb.DB, v *osm.Relation) [][]map[string]string {
	var memberWayLatLons [][]map[string]string

	for _, mem := range v.Members {
		if mem.Type == "way" {

			// lookup from leveldb
			latlons, err := cacheLookupWayNodes(db, mem.Ref)

			// skip way if it fails to denormalize
			if err != nil {
				break
			}

			memberWayLatLons = append(memberWayLatLons, latlons)
		}
	}

	return memberWayLatLons
}

type jsonNode struct {
	ID   osm.NodeID        `json:"id"`
	Type string            `json:"type"`
	Lat  float64           `json:"lat"`
	Lon  float64           `json:"lon"`
	Tags map[string]string `json:"tags"`
}

func onNode(node *osm.Node) {
	marshall := jsonNode{node.ID, "node", node.Lat, node.Lon, node.Tags.Map()}
	json, _ := json.Marshal(marshall)
	fmt.Println(string(json))
}

type jsonWay struct {
	ID   osm.WayID         `json:"id"`
	Type string            `json:"type"`
	Tags map[string]string `json:"tags"`
	// NodeIDs   []int64             `json:"refs"`
	Centroid map[string]string   `json:"centroid"`
	Bounds   map[string]string   `json:"bounds"`
	Nodes    []map[string]string `json:"nodes,omitempty"`
}

func jsonBbox(bounds *geo.Bound) map[string]string {
	// render a North-South-East-West bounding box
	var bbox = make(map[string]string)
	bbox["n"] = strconv.FormatFloat(bounds.North(), 'f', 7, 64)
	bbox["s"] = strconv.FormatFloat(bounds.South(), 'f', 7, 64)
	bbox["e"] = strconv.FormatFloat(bounds.East(), 'f', 7, 64)
	bbox["w"] = strconv.FormatFloat(bounds.West(), 'f', 7, 64)

	return bbox
}

func onWay(way *osm.Way, latlons []map[string]string, centroid map[string]string, bounds *geo.Bound) {
	bbox := jsonBbox(bounds)
	marshall := jsonWay{way.ID, "way", way.Tags.Map() /*, way.NodeIDs*/, centroid, bbox, latlons}
	json, _ := json.Marshal(marshall)
	fmt.Println(string(json))
}

type jsonRelation struct {
	ID       osm.RelationID    `json:"id"`
	Type     string            `json:"type"`
	Tags     map[string]string `json:"tags"`
	Centroid map[string]string `json:"centroid"`
	Bounds   map[string]string `json:"bounds"`
}

func onRelation(relation *osm.Relation, centroid map[string]string, bounds *geo.Bound) {
	bbox := jsonBbox(bounds)
	marshall := jsonRelation{relation.ID, "relation", relation.Tags.Map(), centroid, bbox}
	json, _ := json.Marshal(marshall)
	fmt.Println(string(json))
}

// determine if the node is for an entrance
// https://wiki.openstreetmap.org/wiki/Key:entrance
func isEntranceNode(node *osm.Node) uint8 {
	val := node.Tags.Find("entrance")
	switch strings.ToLower(val) {
	case "main":
		return 2
	case "yes", "home", "staircase":
		return 1
	default:
		return 0
	}
}

// determine if the node is accessible for wheelchair users
// https://wiki.openstreetmap.org/wiki/Key:entrance
func isWheelchairAccessibleNode(node *osm.Node) uint8 {
	val := node.Tags.Find("wheelchair")
	switch strings.ToLower(val) {
	case "yes":
		return 2
	case "no":
		return 0
	default:
		return 1
	}
}

// queue a leveldb write in a batch
func cacheQueueNode(batch *leveldb.Batch, node *osm.Node) {
	id, val := nodeToBytes(node)
	batch.Put([]byte(id), []byte(val))
}

// queue a leveldb write in a batch
func cacheQueueWay(batch *leveldb.Batch, way *osm.Way) {
	id, val := wayToBytes(way)
	batch.Put([]byte(id), []byte(val))
}

// flush a leveldb batch to database and reset batch to 0
func cacheFlush(db *leveldb.DB, batch *leveldb.Batch, sync bool) {
	var writeOpts = &opt.WriteOptions{
		NoWriteMerge: true,
		Sync:         sync,
	}

	err := db.Write(batch, writeOpts)
	if err != nil {
		log.Fatal(err)
	}
	batch.Reset()
}

func cacheLookupNodes(db *leveldb.DB, way *osm.Way) ([]map[string]string, error) {

	var container []map[string]string

	for _, each := range way.Nodes {
		stringid := strconv.FormatInt(int64(each.ID), 10)

		data, err := db.Get([]byte(stringid), nil)
		if err != nil {
			log.Println("[warn] denormalize failed for way:", way.ID, "node not found:", stringid)
			return make([]map[string]string, 0), err
		}

		container = append(container, bytesToLatLon(data))
	}

	return container, nil
}

func cacheLookupWayNodes(db *leveldb.DB, wayid int64) ([]map[string]string, error) {

	// prefix the key with 'W' to differentiate it from node ids
	stringid := "W" + strconv.FormatInt(wayid, 10)

	// look up way bytes
	reldata, err := db.Get([]byte(stringid), nil)
	if err != nil {
		log.Println("[warn] lookup failed for way:", wayid, "noderefs not found:", stringid)
		return make([]map[string]string, 0), err
	}

	// generate a way object
	var way = &osm.Way{
		ID:    osm.WayID(wayid),
		Nodes: bytesToIDSlice(reldata),
	}

	return cacheLookupNodes(db, way)
}

// decode bytes to a 'latlon' type object
func bytesToLatLon(data []byte) map[string]string {
	buf := make([]byte, 0, 8)
	latlon := make(map[string]string, 4)

	// first 6 bytes are the latitude
	buf = append(buf, data[0:6]...)
	lat64 := math.Float64frombits(binary.BigEndian.Uint64(buf[:8]))
	latlon["lat"] = strconv.FormatFloat(lat64, 'f', 7, 64)

	// next 6 bytes are the longitude
	buf = append(buf[:0], data[6:12]...)
	lon64 := math.Float64frombits(binary.BigEndian.Uint64(buf[:8]))
	latlon["lon"] = strconv.FormatFloat(lon64, 'f', 7, 64)

	// check for the bitmask byte which indicates things like an
	// entrance and the level of wheelchair accessibility
	if len(data) > 12 {
		latlon["entrance"] = fmt.Sprintf("%d", (data[12]&0xC0)>>6)
		latlon["wheelchair"] = fmt.Sprintf("%d", (data[12]&0x30)>>4)
	}

	return latlon
}

// encode a node as bytes (between 12 & 13 bytes used)
func nodeToBytes(node *osm.Node) (string, []byte) {
	stringid := strconv.FormatInt(int64(node.ID), 10)

	buf := make([]byte, 14)
	// encode lat/lon as 64 bit floats packed in to 8 bytes,
	// each float is then truncated to 6 bytes because we don't
	// need the additional precision (> 8 decimal places)

	binary.BigEndian.PutUint64(buf, math.Float64bits(node.Lat))
	binary.BigEndian.PutUint64(buf[6:], math.Float64bits(node.Lon))

	// generate a bitmask for relevant tag features
	isEntrance := isEntranceNode(node)
	if isEntrance == 0 {
		return stringid, buf[:12]
	}

	// leftmost two bits are for the entrance, next two bits are accessibility
	// remaining 4 rightmost bits are reserved for future use.
	bitmask := isEntrance << 6
	bitmask |= isWheelchairAccessibleNode(node) << 4
	buf[12] = bitmask

	return stringid, buf[:13]
}

func idSliceToBytes(nodes osm.WayNodes) []byte {
	buf := make([]byte, 8*len(nodes))
	for i := range nodes {
		binary.BigEndian.PutUint64(buf[8*i:], uint64(nodes[i].ID))
	}
	return buf
}

func bytesToIDSlice(bytes []byte) osm.WayNodes {
	if len(bytes)%8 != 0 {
		log.Fatal("invalid byte slice length: not divisible by 8")
	}
	nodes := make(osm.WayNodes, len(bytes)/8)
	for i := range nodes {
		nodes[i].ID = osm.NodeID(binary.BigEndian.Uint64(bytes[i*8:]))
	}
	return nodes
}

// encode a way as bytes (repeated int64 numbers)
func wayToBytes(way *osm.Way) (string, []byte) {
	// prefix the key with 'W' to differentiate it from node ids
	stringid := "W" + strconv.FormatInt(int64(way.ID), 10)
	return stringid, idSliceToBytes(way.Nodes)
}

func openFile(filename string) *os.File {
	// no file specified
	if len(filename) < 1 {
		log.Fatal("invalid file: you must specify a pbf path as arg[1]")
	}
	// try to open the file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func openLevelDB(path string) *leveldb.DB {
	// try to open the db
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// extract all keys to array
// keys := []string{}
// for k := range v.Tags {
//     keys = append(keys, k)
// }

// check tags contain features from a whitelist
func matchTagsAgainstCompulsoryTagList(tags osm.Tags, tagList []string) bool {
	for _, name := range tagList {

		feature := strings.Split(name, "~")
		val := tags.Find(feature[0])
		if val == "" {
			return false
		}

		// value check
		if len(feature) > 1 {
			if val != feature[1] {
				return false
			}
		}
	}

	return true
}

// check tags contain features from a groups of whitelists
func containsValidTags(tags osm.Tags, group map[string][]string) bool {
	for _, list := range group {
		if matchTagsAgainstCompulsoryTagList(tags, list) {
			return true
		}
	}
	return false
}

// trim leading/trailing spaces from keys and values
func trimTags(tags osm.Tags) {
	for i := range tags {
		tags[i].Key = strings.TrimSpace(tags[i].Key)
		tags[i].Value = strings.TrimSpace(tags[i].Value)
	}
}

// check if a tag list is empty or not
func hasTags(tags osm.Tags) bool {
	return len(tags) != 0
}

// select which entrance is preferable
func selectEntrance(entrances []map[string]string) map[string]string {

	// use the mapped entrance location where available
	var centroid = make(map[string]string)
	centroid["type"] = "entrance"

	// prefer the first 'main' entrance we find (should usually only be one).
	for _, entrance := range entrances {
		if val, ok := entrance["entrance"]; ok && val == "2" {
			centroid["lat"] = entrance["lat"]
			centroid["lon"] = entrance["lon"]
			return centroid
		}
	}

	// else prefer the first wheelchair accessible entrance we find
	for _, entrance := range entrances {
		if val, ok := entrance["wheelchair"]; ok && val != "0" {
			centroid["lat"] = entrance["lat"]
			centroid["lon"] = entrance["lon"]
			return centroid
		}
	}

	// otherwise just take the first entrance in the list
	centroid["lat"] = entrances[0]["lat"]
	centroid["lon"] = entrances[0]["lon"]
	return centroid
}

// compute the centroid of a way and its bbox
func computeCentroidAndBounds(latlons []map[string]string) (map[string]string, *geo.Bound) {

	// check to see if there is a tagged entrance we can use.
	var entrances []map[string]string
	for _, latlon := range latlons {
		if _, ok := latlon["entrance"]; ok {
			entrances = append(entrances, latlon)
		}
	}

	// convert lat/lon map to geo.PointSet
	points := geo.NewPointSet()
	for _, each := range latlons {
		var lon, _ = strconv.ParseFloat(each["lon"], 64)
		var lat, _ = strconv.ParseFloat(each["lat"], 64)
		points.Push(geo.NewPoint(lon, lat))
	}

	// use the mapped entrance location where available
	if len(entrances) > 0 {
		return selectEntrance(entrances), points.Bound()
	}

	// determine if the way is a closed centroid or a linestring
	// by comparing first and last coordinates.
	isClosed := false
	if points.Length() > 2 {
		isClosed = points.First().Equals(points.Last())
	}

	// compute the centroid using one of two different algorithms
	var compute *geo.Point
	if isClosed {
		compute = GetPolygonCentroid(points)
	} else {
		compute = GetLineCentroid(points)
	}

	// return point as lat/lon map
	var centroid = make(map[string]string)
	centroid["lat"] = strconv.FormatFloat(compute.Lat(), 'f', 7, 64)
	centroid["lon"] = strconv.FormatFloat(compute.Lng(), 'f', 7, 64)

	return centroid, points.Bound()
}
