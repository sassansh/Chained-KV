<!-- PROJECT LOGO -->
<br />
<p align="center">
 <a href="https://github.com/sassansh/Chained-KV">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>
  <h2 align="center">Chained Key-Value Storage System</h2>

  <p align="center">
     A chained key-value storage system (KVS) with strong data consistency guarantees and support for multiple servers and clients. Built as a group assignment for UBC's <a href="https://courses.students.ubc.ca/cs/courseschedule?pname=subjarea&tname=subj-course&dept=CPSC&course=416">CPSC 416</a> (Distributed Systems).
  </p>
</p>

<p align="center">
    <img src="images/diagram.jpg" alt="Logo" width="300" >
</p>

## Table of Contents

- [Assignment Spec 🎯](#Assignment-spec-)
- [Technology Stack 🛠️](#technology-stack-%EF%B8%8F)
- [Prerequisites 🍪](#prerequisites-)
- [Setup 🔧](#setup-)
- [Team 😁](#team-)

## Assignment Spec 🎯

For a PDF containing the assignent's specifications, please view [assignment-spec.pdf](https://github.com/sassansh/Chained-KV/blob/main/assignment-spec.pdf).

## Technology Stack 🛠️

[Go](https://go.dev)

## Prerequisites 🍪

You should have [GoLand](https://www.jetbrains.com/go/download/), [Go v1.18.1](https://go.dev/dl/) and [Git](https://git-scm.com/) installed on your PC.

## Setup 🔧

1. Clone the repo using:

   ```bash
     git clone https://github.com/sassansh/Chained-KV.git
   ```

2. Open the project in GoLand.

3. First, start the tracing server by running:

   ```bash
     go run cmd/tracing-server/main.go
   ```

4. Next, start the Coord by running:

   ```bash
     go run cmd/coord/main.go
   ```

5. Next, start Servers 1-5 by running:

   ```bash
     go run cmd/server/main.go
     go run cmd/server/main2.go
     go run cmd/server/main3.go
     go run cmd/server/main4.go
     go run cmd/server/main5.go
   ```

6. Lastly, start the Client which will make multiple Push and Get reqeusts by running:

   ```bash
     go run cmd/client/main.go
   ```

## Team 😁

Sassan Shokoohi - [GitHub](https://github.com/sassansh) - [LinkedIn](https://www.linkedin.com/in/sassanshokoohi/) - [Personal Website](https://sassanshokoohi.ca)

Jonathan Hirsch

Naithan Bosse
