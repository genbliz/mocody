import { MyRepository } from "./repo";

let count = 0;

function runInserts() {
  const tout = setInterval(() => {
    count++;
    if (count >= 10) {
      clearInterval(tout);
      process.exit(0);
    } else {
      MyRepository.create()
        .catch((e) => console.log(e))
        .finally(() => console.log(count));
    }
  }, 2000);
}

setTimeout(() => {
  console.log("Initializing");
  runInserts();
}, 10);

// ts-node ./src/test/insert.ts
