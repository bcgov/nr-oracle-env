---
marp: true

style: |
  section.centered {
    display: flex;
    flex-direction: column ;
    justify-content: center;
    text-align: center;
  }

---

# Objective

- *Discuss how Containerized Oracle Databases can be used to identifying objects and relationships in **THE** forestry database...*
- *... Demo idea of "just enough oracle" to mimic the objects required by a Modernization project need in THE schema locally*

---

# Overview
1. Overview of **THE** problem
1. Discuss some database issues and solutions
1. Background on why, where and when containerized oracle fits in.
1. Demo of some of the tooling
1. Discussion / Feedback

---

# Acknowledgements / People I have stolen from

- **Andrew Schwenker** - Worked with DBA's to get initial datapump export, identified steps to load in oracle
- **Paolo Cruz** - Demo and deployment of oracle for forest client
- **Derek Roberts** - Always helps when I have questions

 ---

# The Modernization Problem

<style>
td, th {
   border: none!important;
}
</style>

 <table >
 <tr>
    <td><b style="font-size:30px">where we are</b></td>
    <td><b style="font-size:30px">  &nbsp;   </b></td>
    <td><b style="font-size:30px">dream state</b></td>
 </tr>
 <tr>
    <td><img src="https://dataedo.com/asset/img/banners/blog/huge_erd_pacman.png" width="400px" >
    </td>
    <td> <img src="https://www.di-da.eus/wp-content/uploads/2018/07/arrow-icon-28.png" width="200px"> </td>
    <td><img src="https://i0.wp.com/thesametech.com/wp-content/uploads/2023/10/image-2.png?w=1431&ssl=1" width="400px">
</td>
 </tr>
 <tr>
    <td>the <b>THE</b> schema</td>
    <td>&nbsp;</td>
    <td>Apps and their data separated by business domain</td>
  </tr>

</table>

<!-- Speaker Notes:
- difficult to predict the impacts of even small changes to THE database.

-->

---

# Questions that arise on the journey...

- Are you kidding me? Is there **REALLY** only one schema?
- Which of those tables are used by my application?
- What other applications use my applications tables?
- What other tables are required to maintain referential integrity?

- Which eventually leads to...

    *  Can I get access to that database to help answer some of these questions?

---

<style>
centerimage {
  display: block;
  margin-left: auto;
  margin-right: auto;
  width: 40%;
  text-align: center;
    position: absolute;

}
</style>

#    ... and the answer is... **NO**


![w:400 center](https://i.imgflip.com/5mgnsc.jpg)



---

<style>
centerimage {
  display: block;
  margin-left: auto;
  margin-right: auto;
  width: 40%;
  text-align: center;
    position: absolute;

}
img[alt~="center"] {
  display: block;
  margin: 0 auto;
}
h1 {
  text-align: center;
}
h4 {
   text-align: center;
   font-size: 25px
}


</style>

# Local Oracle / THE Database

![w:300 center](https://blogger.googleusercontent.com/img/a/AVvXsEjnOwvsBwhbGXEYs0qVExLjr0rhtYzE0KaBJg3qduxL3n-ewqyetqb4ouGkAN4skX07F4yQAb1cOvAqcJThBTZ_vg4U8bk3a9Ogrr5ooPju8jg5Iwq4XgWl-MNsVbKGPcK7yY2WwvgWWAMSJBNhtplxbs0Wm0RBvVlnXxTrxEjJLEpGbYSVrH62pQ=w1200-h630-p-k-no-nu)

- Hosted databases (TEST/PROD) have strange unknowable access policies
- Working through bureaucracy to gain necessary access can be costly / time consuming
- Access that is provided frequently does not meet requirements for modernization teams
- **Workaround:** host your own oracle database!

---

# Structural Oracle Database

- DBA has created an export of all THE structure.
- Docker compose:
    - starts the database
    - loads all the objects
    - grants god like database access
- Tool / Script created to identify dependency tree of a database table.
- Database does not include any data, only structure.
- Used to identify what data is required.

---

# Review of Phases of Modernization
#### Illustrate where and when containerized oracle might be useful

- Original state - Old app untouched
- First deploy of a modernized app
- Approaching initial release - addition of data sync
- How dockerized oracle fits in

---

#### [Dev Process - Pre-modernization](https://viewer.diagrams.net/?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1#G1Gih_uzk3zgEs4cj7jeb8lprJy9mdnHto)

- #### This phase might use the structural database to understand what objects are used by the app that is being modernized.

<br>

<!-- <img src="./pre-fds.drawio.svg"> -->
![w:700 center](./pre-fds.drawio.svg)

<br>
<br>

<sub><sup>* huge over simplification!</sup></sub>

---

#### [Dev Process - First deployments](https://viewer.diagrams.net/?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1#G1Cwna4WZi44J-J4i59xiVEQPm0ylyxgaf)

<img src="./fds-start.drawio.svg">
<br>

---

#### [Version 1 - Initial release](https://viewer.diagrams.net/?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1#G1x5tmujs9EpnHUNRPR0UqXKTKLZQOh7jV)

<img src="./fds-dbpInt.drawio.svg" width="800px">

---

#### [Local Oracle Dev envs](https://viewer.diagrams.net/?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1#G1ptFHF446J13LBPVl7MGvCnq--D_r50Yu)

<img src="./fds_local_oracle.drawio.svg" width="650px">


---

# Demo

- oracle THE database
- dependency report
- generate migrations

---

# Discussion / Feedback

<!-- <img src="https://lymansblog.wordpress.com/wp-content/uploads/2014/10/create-shareable-content.jpg" width="400px"> -->

![w:300 center](https://lymansblog.wordpress.com/wp-content/uploads/2014/10/create-shareable-content.jpg)


- Is any of this useful?
- Is there value in making it easier to use?
- Any feedback Welcome!

