import { MyRepository } from "./repo";

function runInserts() {
  MyRepository.getIt()
    .then((d) => console.log(d))
    .catch((e) => console.log(e));
}

setTimeout(() => {
  console.log("Getting...");
  runInserts();
}, 10);

// ts-node ./src/test/get-data.ts
